# BulkWriter Tier 1 — FloatVector Fast Path Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a patched `milvus-sdk-java` 2.6.8 fork (`2.6.8-vts.1`) that replaces Gson reflective deserialization with a hand-written JsonArray walk in `ParamUtils.checkFieldValue` FloatVector branch, so VTS can benchmark the single-change impact end-to-end.

**Architecture:** Clone `milvus-io/milvus-sdk-java` at tag `v2.6.8` into `~/work/milvus-sdk-java-vts/`, branch `vts/float-vector-fast-path`, bump version to `2.6.8-vts.1`, apply the one-file code change, run the existing `BulkWriterTest` regression as a safety net, `mvn install` to local `~/.m2`, then flip the two VTS `connector-milvus` pom version strings as an **uncommitted** working-tree edit.

**Tech Stack:** Java 8+, Maven (via wrapper `./mvnw`), Gson 2.x (shipped with the SDK), JUnit 5 (`BulkWriterTest`).

**Spec:** [docs/superpowers/specs/2026-04-09-bulkwriter-tier1-float-vector-design.md](../specs/2026-04-09-bulkwriter-tier1-float-vector-design.md)

---

## File Structure

**New directory:**
- `~/work/milvus-sdk-java-vts/` — local fork of `milvus-io/milvus-sdk-java` at `v2.6.8`, on branch `vts/float-vector-fast-path`.

**Modified in fork:**
- `~/work/milvus-sdk-java-vts/pom.xml` — parent pom, version bumped to `2.6.8-vts.1` by `versions:set`.
- `~/work/milvus-sdk-java-vts/sdk-core/pom.xml` — child pom, version bumped (and parent reference bumped) by `versions:set`.
- `~/work/milvus-sdk-java-vts/sdk-bulkwriter/pom.xml` — child pom, version bumped (and parent reference bumped) by `versions:set`.
- `~/work/milvus-sdk-java-vts/examples/pom.xml` — child pom (parent reference only), bumped by `versions:set`.
- `~/work/milvus-sdk-java-vts/tests/milvustest/pom.xml` — same.
- `~/work/milvus-sdk-java-vts/tests/milvustestv2/pom.xml` — same.
- `~/work/milvus-sdk-java-vts/sdk-core/src/main/java/io/milvus/param/ParamUtils.java` — **this is the actual code change**. FloatVector branch of `checkFieldValue`, lines ~344–360.

**Regression test used (no new test file created):**
- `~/work/milvus-sdk-java-vts/sdk-bulkwriter/src/test/java/io/milvus/bulkwriter/BulkWriterTest.java` — already covers FloatVector happy path (dim 128 vector appended via `appendRow`) and error paths (dim mismatch, wrong type) at roughly lines 328, 440–504. We rely on this suite as the safety net.

**Modified in VTS (uncommitted working-tree edit):**
- `/Users/shawn.he/work/vts/seatunnel-connectors-v2/connector-milvus/pom.xml` — two `<version>` elements flipped from `2.6.8` to `2.6.8-vts.1` (lines 44 and 55).

**Not modified:** VTS `MilvusSinkConverter.java`, `MilvusBulkWriter.java`, or any other connector code. The spec explicitly excludes VTS-side changes from Iteration 1.

---

## Task 1: Set up the local fork

**Files:**
- Create: `~/work/milvus-sdk-java-vts/` (via `git clone`)

- [ ] **Step 1.1: Verify parent directory exists and target doesn't**

```bash
ls -d ~/work
ls -d ~/work/milvus-sdk-java-vts 2>&1 || echo "OK not present"
```

Expected: `~/work` exists; `~/work/milvus-sdk-java-vts` does NOT exist. If it already exists, STOP and ask the user — the fork may already be set up from a prior iteration attempt.

- [ ] **Step 1.2: Clone the upstream SDK repo**

```bash
cd ~/work && git clone https://github.com/milvus-io/milvus-sdk-java.git milvus-sdk-java-vts
```

Expected: clone succeeds; `~/work/milvus-sdk-java-vts/pom.xml` exists.

- [ ] **Step 1.3: Check out the v2.6.8 tag and cut a branch**

```bash
cd ~/work/milvus-sdk-java-vts && git checkout v2.6.8 && git switch -c vts/float-vector-fast-path
```

Expected: detached HEAD briefly on `v2.6.8`, then on branch `vts/float-vector-fast-path`.

- [ ] **Step 1.4: Sanity-check the target file exists at the expected path**

```bash
ls ~/work/milvus-sdk-java-vts/sdk-core/src/main/java/io/milvus/param/ParamUtils.java
```

Expected: the file exists. If the path differs from the plan, STOP — the upstream repo layout may have changed between when the plan was written and now.

---

## Task 2: Establish a green test baseline before any code change

Run the existing BulkWriterTest on the unmodified fork. A green run here proves the local Maven/JDK toolchain works and gives us a before/after anchor for the behavioral regression check in Task 4. Not all tests in `BulkWriterTest` require Docker — some cover in-process `LocalBulkWriter` behavior. We target those.

**Files:** None modified in this task.

- [ ] **Step 2.1: Confirm Java + Maven wrapper work**

```bash
cd ~/work/milvus-sdk-java-vts && ./mvnw --version
```

Expected: prints Maven version and Java version (Java 8 or later).

- [ ] **Step 2.2: Compile the whole project once so subsequent test runs are fast**

```bash
cd ~/work/milvus-sdk-java-vts && ./mvnw -DskipTests install
```

Expected: BUILD SUCCESS. On first run this will download a lot of Maven dependencies (several minutes). If this fails, STOP and debug the toolchain before going further.

Note: this installs a `2.6.8` build to `~/.m2`. That's intentional — it overlays the upstream artifact with a locally-built identical copy. No version number change yet.

- [ ] **Step 2.3: Identify a LocalBulkWriter FloatVector test to run as baseline**

```bash
grep -n "LocalBulkWriter\|localBulkWriter\|FloatVector" ~/work/milvus-sdk-java-vts/sdk-bulkwriter/src/test/java/io/milvus/bulkwriter/BulkWriterTest.java | head -40
```

Expected: hits around `LocalBulkWriter` / `localBulkWriter` that do in-process FloatVector append. Note the enclosing `@Test` method name(s) — e.g. `testLocalBulkWriter` or similar.

- [ ] **Step 2.4: Run the BulkWriterTest test class**

```bash
cd ~/work/milvus-sdk-java-vts && ./mvnw -pl sdk-bulkwriter -am test -Dtest=BulkWriterTest
```

Expected: BUILD SUCCESS, all tests in `BulkWriterTest` either PASS or are SKIPPED (some require Docker/MinIO and auto-skip when unavailable). **Key requirement:** no FAILURE or ERROR lines. If a Docker-dependent test fails because Docker isn't running, that's expected and OK — what we care about is that the **local, in-process** FloatVector test runs green.

If some tests fail for non-Docker reasons, investigate before proceeding. Do NOT move to Task 3 on a red baseline.

- [ ] **Step 2.5: Record the baseline test result**

Jot down which tests ran, which passed, which were skipped. This is the "before" state that the same command in Task 4 Step 4.3 must match.

---

## Task 3: Bump version + apply the code change

**Files:**
- Modify: `~/work/milvus-sdk-java-vts/pom.xml` (via `versions:set`)
- Modify: `~/work/milvus-sdk-java-vts/sdk-core/pom.xml` (via `versions:set`)
- Modify: `~/work/milvus-sdk-java-vts/sdk-bulkwriter/pom.xml` (via `versions:set`)
- Modify: `~/work/milvus-sdk-java-vts/examples/pom.xml` (via `versions:set`)
- Modify: `~/work/milvus-sdk-java-vts/tests/milvustest/pom.xml` (via `versions:set`)
- Modify: `~/work/milvus-sdk-java-vts/tests/milvustestv2/pom.xml` (via `versions:set`)
- Modify: `~/work/milvus-sdk-java-vts/sdk-core/src/main/java/io/milvus/param/ParamUtils.java` lines 344–360

- [ ] **Step 3.1: Bump version to `2.6.8-vts.1` atomically across all modules**

```bash
cd ~/work/milvus-sdk-java-vts && ./mvnw versions:set -DnewVersion=2.6.8-vts.1 -DgenerateBackupPoms=false -DprocessAllModules=true
```

Expected: BUILD SUCCESS. All six `pom.xml` files now reference `2.6.8-vts.1` in their `<version>` and/or `<parent><version>` elements.

The `-DprocessAllModules=true` flag is essential — without it, `versions:set` only touches the aggregator pom and child modules keep pointing at `2.6.8`.

- [ ] **Step 3.2: Verify the version bump hit all poms**

```bash
cd ~/work/milvus-sdk-java-vts && grep -rn "2.6.8-vts.1" pom.xml sdk-core/pom.xml sdk-bulkwriter/pom.xml | head
```

Expected: at least one hit per file. If `sdk-core/pom.xml` or `sdk-bulkwriter/pom.xml` still show `2.6.8` in their `<parent>` section, something went wrong — STOP and investigate.

- [ ] **Step 3.3: Read the FloatVector block to confirm line numbers**

Read `~/work/milvus-sdk-java-vts/sdk-core/src/main/java/io/milvus/param/ParamUtils.java` lines 340–365. Confirm you see the exact "Before" block from the spec. If the lines differ because upstream shifted, adjust the edit target but NOT the change itself.

- [ ] **Step 3.4: Apply the code change**

Replace the FloatVector case body. Old code:

```java
            case FloatVector: {
                if (!(value.isJsonArray())) {
                    throw new ParamException(String.format(errMsgs.get(dataType), fieldName));
                }
                try {
                    List<Float> vector = JsonUtils.fromJson(value, new TypeToken<List<Float>>() {
                    }.getType());
                    if (vector.size() != dim) {
                        String msg = "Incorrect dimension for field '%s': dimension: %d is not equal to field's dimension: %d";
                        throw new ParamException(String.format(msg, fieldName, vector.size(), dim));
                    }
                    return vector; // return List<Float> for genFieldData()
                } catch (JsonSyntaxException e) {
                    throw new ParamException(String.format("Unable to convert JsonArray to List<Float> for field '%s'. Reason: %s",
                            fieldName, e.getCause().getMessage()));
                }
            }
```

New code:

```java
            case FloatVector: {
                if (!(value.isJsonArray())) {
                    throw new ParamException(String.format(errMsgs.get(dataType), fieldName));
                }
                JsonArray arr = value.getAsJsonArray();
                int n = arr.size();
                if (n != dim) {
                    String msg = "Incorrect dimension for field '%s': dimension: %d is not equal to field's dimension: %d";
                    throw new ParamException(String.format(msg, fieldName, n, dim));
                }
                List<Float> vector = new ArrayList<>(n);
                try {
                    for (int i = 0; i < n; i++) {
                        vector.add(arr.get(i).getAsFloat());
                    }
                } catch (NumberFormatException | IllegalStateException | UnsupportedOperationException e) {
                    throw new ParamException(String.format(
                            "Unable to convert JsonArray to List<Float> for field '%s'. Reason: %s",
                            fieldName, e.getMessage()));
                }
                return vector; // return List<Float> for genFieldData()
            }
```

Do NOT change the `TypeToken` or `JsonUtils.fromJson` usage in any other branch of the switch. Do NOT add or remove imports — `JsonArray` and `ArrayList` are already imported in `ParamUtils.java`.

- [ ] **Step 3.5: Verify imports are already present**

```bash
grep -n "^import" ~/work/milvus-sdk-java-vts/sdk-core/src/main/java/io/milvus/param/ParamUtils.java | grep -E "JsonArray|ArrayList"
```

Expected: both `com.google.gson.JsonArray` (or `com.google.gson.*`) and `java.util.ArrayList` show up. If either is missing, add it to the import block at the top of the file — but first double-check with `grep -n "JsonArray\|ArrayList" ParamUtils.java` that the file wasn't already using them via wildcard imports.

- [ ] **Step 3.6: Compile the modified module only**

```bash
cd ~/work/milvus-sdk-java-vts && ./mvnw -pl sdk-core compile
```

Expected: BUILD SUCCESS. If compile fails with "cannot find symbol JsonArray" or "cannot find symbol ArrayList", go back to Step 3.5 and add the missing import.

---

## Task 4: Run regression tests on the patched code

Re-run the same test command from Task 2 Step 2.4. The set of passing/failing/skipped tests must be identical to the baseline recorded in Step 2.5.

**Files:** None modified in this task.

- [ ] **Step 4.1: Run BulkWriterTest against the patched sdk-core**

```bash
cd ~/work/milvus-sdk-java-vts && ./mvnw -pl sdk-bulkwriter -am test -Dtest=BulkWriterTest
```

Expected: same PASS/SKIP pattern as baseline (Step 2.5). No new FAILURES.

- [ ] **Step 4.2: If a test newly fails, diagnose before proceeding**

Likely causes if something goes red:
- Exception message format changed enough that an `assertEquals` on the message text fails → the test was asserting the exact Gson-generated reason string. Update the exception message in `ParamUtils.java` to match the old text format as closely as possible, or (less ideal) update the test assertion.
- `assertThrows(MilvusException.class, ...)` stopped matching → the new code is throwing a different exception subtype. `ParamException` extends `MilvusException`, so this shouldn't happen. If it does, the `catch (NumberFormatException | IllegalStateException | UnsupportedOperationException e)` clause isn't catching something it should. Read the stack trace.

Do NOT move on with a red baseline.

- [ ] **Step 4.3: Diff-check: confirm the only file changes are the ones the plan intended**

```bash
cd ~/work/milvus-sdk-java-vts && git diff --stat
```

Expected: `sdk-core/src/main/java/io/milvus/param/ParamUtils.java` plus the six pom.xml files from `versions:set`. Nothing else.

---

## Task 5: Install the patched SDK to local `~/.m2`

**Files:** None modified. This task produces jars in `~/.m2/repository/io/milvus/`.

- [ ] **Step 5.1: `mvn clean install` with tests skipped**

```bash
cd ~/work/milvus-sdk-java-vts && ./mvnw clean install -DskipTests
```

Expected: BUILD SUCCESS across all modules. Tests are skipped because Task 4 already covered regression.

- [ ] **Step 5.2: Verify both jars landed in the expected paths**

```bash
ls -la ~/.m2/repository/io/milvus/milvus-sdk-java/2.6.8-vts.1/milvus-sdk-java-2.6.8-vts.1.jar
ls -la ~/.m2/repository/io/milvus/milvus-sdk-java-bulkwriter/2.6.8-vts.1/milvus-sdk-java-bulkwriter-2.6.8-vts.1.jar
```

Expected: both jars exist, recently modified. Both are required for VTS — `milvus-sdk-java-2.6.8-vts.1.jar` is the one that contains the patched `ParamUtils` class.

- [ ] **Step 5.3: Sanity-check the patched class is actually in the core jar**

```bash
unzip -p ~/.m2/repository/io/milvus/milvus-sdk-java/2.6.8-vts.1/milvus-sdk-java-2.6.8-vts.1.jar io/milvus/param/ParamUtils.class | javap -p -c /dev/stdin 2>/dev/null | grep -c "getAsFloat"
```

Expected: a count of at least 1 (the new hand-written `arr.get(i).getAsFloat()` loop). If the count is 0, the build used a stale compile and did not pick up your edit — `mvn clean install` again.

---

## Task 6: Commit the fork change

**Files:**
- Commit: `~/work/milvus-sdk-java-vts/` all staged changes on branch `vts/float-vector-fast-path`.

- [ ] **Step 6.1: Review staged diff one more time**

```bash
cd ~/work/milvus-sdk-java-vts && git diff --stat && git diff sdk-core/src/main/java/io/milvus/param/ParamUtils.java
```

Expected: the stat shows the six poms + ParamUtils.java; the ParamUtils.java diff matches exactly what Task 3 specified.

- [ ] **Step 6.2: Commit with a descriptive message**

```bash
cd ~/work/milvus-sdk-java-vts && git add -u && git commit -m "$(cat <<'EOF'
[perf] FloatVector fast path in ParamUtils.checkFieldValue

Replace JsonUtils.fromJson(..., TypeToken<List<Float>>) with a manual
JsonArray walk to skip Gson's ReflectiveTypeAdapterFactory on the
BulkWriter row-append hot path. Also bump version to 2.6.8-vts.1.

Part of VTS Tier 1 bulkwriter optimization iteration 1.
EOF
)"
```

Expected: commit succeeds on branch `vts/float-vector-fast-path`.

- [ ] **Step 6.3: Do NOT push**

The spec explicitly keeps this fork local-only. Do not `git push`.

---

## Task 7: Flip VTS connector-milvus pom to the patched version

**Files:**
- Modify: `/Users/shawn.he/work/vts/seatunnel-connectors-v2/connector-milvus/pom.xml` lines 44 and 55.

This edit is deliberately **not committed** — see spec §4 rationale.

- [ ] **Step 7.1: Read the current pom to confirm the target lines**

Read `/Users/shawn.he/work/vts/seatunnel-connectors-v2/connector-milvus/pom.xml` lines 40–60.

Expected: see the two `<version>2.6.8</version>` occurrences under `milvus-sdk-java` (line 44) and `milvus-sdk-java-bulkwriter` (line 55).

- [ ] **Step 7.2: Bump both versions to `2.6.8-vts.1`**

Edit `/Users/shawn.he/work/vts/seatunnel-connectors-v2/connector-milvus/pom.xml`:

Change the `<version>2.6.8</version>` inside the `milvus-sdk-java` `<dependency>` block to `<version>2.6.8-vts.1</version>`.
Change the `<version>2.6.8</version>` inside the `milvus-sdk-java-bulkwriter` `<dependency>` block to `<version>2.6.8-vts.1</version>`.

- [ ] **Step 7.3: Verify the edit**

```bash
grep -n "2.6.8" /Users/shawn.he/work/vts/seatunnel-connectors-v2/connector-milvus/pom.xml
```

Expected: both hits should show `2.6.8-vts.1`. No unqualified `2.6.8` should remain for these two dependencies. (Other parts of the pom may mention `2.3.11-SNAPSHOT` for the VTS parent — that is unrelated, leave it alone.)

- [ ] **Step 7.4: Verify the edit is uncommitted**

```bash
cd /Users/shawn.he/work/vts && git status seatunnel-connectors-v2/connector-milvus/pom.xml
```

Expected: `modified: seatunnel-connectors-v2/connector-milvus/pom.xml` showing as an unstaged working-tree change. Do NOT `git add` or `git commit` this file.

---

## Task 8: Build VTS with the patched SDK

**Files:** None modified. This task produces a distribution tarball under `seatunnel-dist/target/`.

- [ ] **Step 8.1: Run the standard dist build**

```bash
cd /Users/shawn.he/work/vts && mvnd clean package -pl :seatunnel-dist -am -D"skip.ui"=true -Dmaven.test.skip=true -Prelease
```

Expected: BUILD SUCCESS. Maven pulls `milvus-sdk-java:2.6.8-vts.1` and `milvus-sdk-java-bulkwriter:2.6.8-vts.1` from the local `~/.m2` populated in Task 5.

- [ ] **Step 8.2: If Maven complains it cannot find `2.6.8-vts.1`**

Most likely cause: Task 5 Step 5.1 silently failed for one module, or `versions:set` in Task 3 Step 3.1 didn't process all modules. Go back and verify both jars exist in `~/.m2` (Task 5 Step 5.2). If they do, try `mvnd … -U` to force Maven to re-resolve.

- [ ] **Step 8.3: Confirm the built VTS dist includes the patched class**

```bash
find /Users/shawn.he/work/vts/seatunnel-dist/target -name "connector-milvus*.jar" -exec sh -c 'echo "=== {} ==="; unzip -p "{}" milvus/com/google/gson/JsonArray.class 2>/dev/null | head -c 4' \;
```

Note: because of the `maven-shade-plugin` relocation in `connector-milvus/pom.xml` (lines 78–96), Gson is repackaged under `milvus.com.google.gson`. The presence of `milvus/com/google/gson/JsonArray.class` inside the jar confirms shading happened.

Also sanity-check the patched `ParamUtils`:

```bash
find /Users/shawn.he/work/vts/seatunnel-dist/target -name "connector-milvus*.jar" -exec sh -c 'echo "=== {} ==="; unzip -l "{}" | grep -i "ParamUtils"' \;
```

Expected: see `io/milvus/param/ParamUtils.class` inside the shaded connector jar.

And confirm the patched bytecode actually made it all the way through the shade plugin (mirror of Task 5 Step 5.3, but against the VTS-built shaded connector jar):

```bash
find /Users/shawn.he/work/vts/seatunnel-dist/target -name "connector-milvus*.jar" | while read -r jar; do
  echo "=== $jar ==="
  unzip -p "$jar" io/milvus/param/ParamUtils.class 2>/dev/null \
    | javap -p -c /dev/stdin 2>/dev/null \
    | grep -c "getAsFloat"
done
```

Expected: `getAsFloat` count ≥ 1 for the connector-milvus shaded jar. Count of 0 means the shade plugin pulled an unpatched class from somewhere else in the classpath (e.g., a leftover unshaded 2.6.8 jar) — STOP and investigate `~/.m2` for stray `2.6.8` artifacts lingering alongside `2.6.8-vts.1`.

---

## Task 9: Handoff summary to the user

**Files:** None modified. This task produces a text summary for the user.

- [ ] **Step 9.1: Print the handoff summary**

Tell the user (in the conversation, not a file):

```
Iteration 1 ready to benchmark.

Patched SDK:
  Fork     : ~/work/milvus-sdk-java-vts/
  Branch   : vts/float-vector-fast-path
  Version  : 2.6.8-vts.1
  Installed: ~/.m2/repository/io/milvus/milvus-sdk-java{,-bulkwriter}/2.6.8-vts.1/

VTS changes (working tree only, NOT committed):
  seatunnel-connectors-v2/connector-milvus/pom.xml
    line 44: milvus-sdk-java        2.6.8 → 2.6.8-vts.1
    line 55: milvus-sdk-java-bulkwriter 2.6.8 → 2.6.8-vts.1

Built dist:
  seatunnel-dist/target/apache-seatunnel-*.tar.gz

To run the patched version in the test environment:
  1. Ship seatunnel-dist/target/apache-seatunnel-*.tar.gz to the test host.
  2. Run your benchmark job.

To run the baseline (2.6.8 unmodified) for comparison:
  1. cd ~/work/vts && git checkout -- seatunnel-connectors-v2/connector-milvus/pom.xml
  2. mvnd clean package -pl :seatunnel-dist -am -D"skip.ui"=true -Dmaven.test.skip=true -Prelease
  3. Ship the resulting tarball.

One iteration in flight. Tell me the result and which item to tackle next
(see spec §5 for the iteration protocol and Tier 1 items b–g).
```

---

## Rollback Instructions

At any point, to revert VTS to the baseline SDK:

```bash
cd /Users/shawn.he/work/vts && git checkout -- seatunnel-connectors-v2/connector-milvus/pom.xml
mvnd clean package -pl :seatunnel-dist -am -D"skip.ui"=true -Dmaven.test.skip=true -Prelease
```

To completely discard the fork:

```bash
rm -rf ~/work/milvus-sdk-java-vts
# The 2.6.8-vts.1 jars remain in ~/.m2 but are harmless once the VTS pom is reverted.
# To also clean them up:
rm -rf ~/.m2/repository/io/milvus/milvus-sdk-java{,-bulkwriter}/2.6.8-vts.1/
```

---

## Success Criteria (from spec acceptance criteria)

1. `~/work/milvus-sdk-java-vts/` exists with branch `vts/float-vector-fast-path` containing the version bump + code change commits. (Task 6)
2. Both patched artifacts are present in `~/.m2`:
   - `milvus-sdk-java/2.6.8-vts.1/milvus-sdk-java-2.6.8-vts.1.jar`
   - `milvus-sdk-java-bulkwriter/2.6.8-vts.1/milvus-sdk-java-bulkwriter-2.6.8-vts.1.jar`
   (Task 5)
3. `seatunnel-connectors-v2/connector-milvus/pom.xml` has the two version bumps applied (uncommitted). (Task 7)
4. VTS builds successfully with the standard dist command. (Task 8)
5. Handoff summary delivered. (Task 9)
