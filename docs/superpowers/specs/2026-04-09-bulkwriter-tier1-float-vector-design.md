# Milvus BulkWriter Tier 1 Optimization: FloatVector Fast Path

**Date:** 2026-04-09
**Status:** Design approved, pending implementation
**Scope:** Iteration 1 of a multi-iteration optimization effort

## Context

VTS depends on `io.milvus:milvus-sdk-java-bulkwriter:2.6.8` to write vector data as Parquet files for Milvus bulk import. Profiling of the `BulkWriter.appendRow` hot path identified several JSON-related bottlenecks. See the prior analysis in conversation for the full breakdown; the largest single contributor for FloatVector-heavy workloads is:

```java
// io.milvus.param.ParamUtils.checkFieldValue, FloatVector branch (line ~349)
List<Float> vector = JsonUtils.fromJson(value, new TypeToken<List<Float>>(){}.getType());
```

Gson's `fromJson(JsonElement, Type)` walks the already-parsed `JsonArray` through `ReflectiveTypeAdapterFactory`, allocating boxed `Float` wrappers and intermediate objects per element, per row. For a 1536-dim vector this is ~24 KB of heap churn per row on top of reflection overhead.

This design specifies **Iteration 1** of a multi-step optimization plan: replace this single call site with a hand-written JsonArray walk. No other changes are in scope for this iteration — the point is to isolate the signal from this one change before deciding where to invest next.

## Goals

1. Eliminate Gson reflective deserialization for the FloatVector parsing path in `ParamUtils.checkFieldValue`.
2. Preserve the existing return-value contract (`List<Float>`) and error-reporting contract (`ParamException` with field-name context).
3. Ship a patched SDK jar that VTS can consume locally and benchmark against the unmodified 2.6.8 baseline.
4. Establish a repeatable iteration protocol so subsequent Tier 1 items (binary vectors, sparse vectors, arrays, etc.) can be benchmarked one-at-a-time using the same workflow.

## Non-Goals

- Not changing other vector type branches (Binary, Float16, BFloat16, Int8, Sparse) — deferred to later iterations.
- Not changing Array type branches — deferred.
- Not statically caching `TypeToken` instances elsewhere — deferred (even though it's cheap, it would pollute the measurement signal for this iteration).
- Not touching `BulkWriter.verifyRow` (the `dynamicValues.toString()` and per-row JSON-field `toString()` hotspots) — those require a `FormatFileWriter` interface change and belong to a later iteration.
- Not modifying `ParquetFileWriter.addFloatArray` (the per-element `SimpleGroup` allocation) — that is Tier 3 territory and is a much larger refactor.
- Not modifying the VTS-side `MilvusSinkConverter.buildMilvusData` path, even though it also does a redundant `toJsonTree` round-trip. Iteration 1 is strictly about the SDK side.
- Not writing a JMH microbenchmark; benchmarking is done end-to-end by the user in their own test environment.
- Not publishing the patched SDK to any remote repository. Local-only.
- Not committing the VTS pom version bump to master.

## Design

### Code Change

Single file: `sdk-core/src/main/java/io/milvus/param/ParamUtils.java`, inside `checkFieldValue`, the `case FloatVector` branch. (The upstream `milvus-sdk-java` v2.6.8 repo is a multi-module Maven project; the `io.milvus.param` package lives in the `sdk-core` module, which the `sdk-bulkwriter` module transitively depends on.)

**Before:**

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
        return vector;
    } catch (JsonSyntaxException e) {
        throw new ParamException(String.format("Unable to convert JsonArray to List<Float> for field '%s'. Reason: %s",
                fieldName, e.getCause().getMessage()));
    }
}
```

**After:**

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
    return vector;
}
```

### Contract Preservation

- **Return type:** still `List<Float>`. Downstream `ParquetFileWriter.addFloatArray` casts to `List<Float>`; that cast continues to succeed with `ArrayList<Float>`.
- **Dimension check:** moved before allocation, saving a pointless full-capacity ArrayList on the error path. The error message format is preserved verbatim so any log-scrapers continue to match.
- **Error reporting:** the original code catches `JsonSyntaxException`. The new code catches the three exception types `JsonPrimitive.getAsFloat()` can raise:
  - `NumberFormatException` — element is a non-numeric string primitive.
  - `IllegalStateException` — element is not a `JsonPrimitive` (e.g., nested array, object).
  - `UnsupportedOperationException` — element is `JsonNull`.
  All three are wrapped in a `ParamException` with the same `"Unable to convert JsonArray to List<Float> for field '%s'..."` prefix so error handling upstream stays identical.
- **Incidental fix:** the new code uses `e.getMessage()` directly, which avoids a latent NPE in the upstream code — the original `e.getCause().getMessage()` would NPE if `JsonSyntaxException.getCause()` was null. The new exception types are thrown directly (not wrapped), so no `getCause()` chain is needed.

### Deliberate Non-Changes

- `TypeToken` imports and usages in other branches of `checkFieldValue` remain untouched.
- `JsonUtils.fromJson` calls elsewhere in the file remain untouched.
- No new dependencies, no new imports beyond `JsonArray` / `ArrayList` which are already imported in the file.

## Fork Setup

Local fork at `~/work/milvus-sdk-java-vts/`, derived from upstream `milvus-io/milvus-sdk-java` tag `v2.6.8`.

```bash
cd ~/work
git clone https://github.com/milvus-io/milvus-sdk-java.git milvus-sdk-java-vts
cd milvus-sdk-java-vts
git checkout v2.6.8
git switch -c vts/float-vector-fast-path
./mvnw versions:set -DnewVersion=2.6.8-vts.1 -DgenerateBackupPoms=false
# apply code change to src/main/java/io/milvus/param/ParamUtils.java
./mvnw clean install -DskipTests
git add -u && git commit -m "[perf] FloatVector fast path in ParamUtils.checkFieldValue"
```

Rationale for `mvnw versions:set`: the SDK is a multi-module Maven project. Both `io.milvus:milvus-sdk-java` and `io.milvus:milvus-sdk-java-bulkwriter` share a parent pom, and `versions:set` updates the parent and all children atomically to `2.6.8-vts.1`.

Rationale for `-DskipTests`: the point of Iteration 1 is to get a jar into the user's test environment quickly. If regression confidence is needed the user can rerun without `-DskipTests` — the existing `ParamUtilsTest` / `BulkWriterTest` already cover FloatVector parsing.

## VTS Integration

Two dependency declarations in `seatunnel-connectors-v2/connector-milvus/pom.xml` must be bumped from `2.6.8` to `2.6.8-vts.1`:

```xml
<dependency>
    <groupId>io.milvus</groupId>
    <artifactId>milvus-sdk-java</artifactId>
    <version>2.6.8-vts.1</version>
    ...
</dependency>
<dependency>
    <groupId>io.milvus</groupId>
    <artifactId>milvus-sdk-java-bulkwriter</artifactId>
    <version>2.6.8-vts.1</version>
</dependency>
```

These changes remain **uncommitted** in the working tree. Rationale: the patched jar lives only in the user's local `~/.m2`; if the pom change were committed to master, CI and other developers would hit an unresolvable artifact. Rollback to baseline is a `git checkout -- seatunnel-connectors-v2/connector-milvus/pom.xml`.

Build command (unchanged from project docs):

```bash
mvnd clean package -pl :seatunnel-dist -am -D"skip.ui"=true -Dmaven.test.skip=true -Prelease
```

## Iteration Protocol

Since multiple iterations are expected, each iteration follows the same 5-step cycle:

1. **Code change** on the current `vts/*` branch in the fork — append one commit describing the variant.
2. **Version bump** `2.6.8-vts.N` → `2.6.8-vts.(N+1)`. Incrementing (not overwriting) keeps Maven's local cache honest and lets the user swap between any two iterations by editing only the VTS pom version string.
3. **`./mvnw clean install -DskipTests`** to publish to local `~/.m2`.
4. **Bump VTS pom** to the new version (local edit only).
5. **Handoff** — summarize diff, current `vts-version`, build command, and rollback instructions.

Baseline comparison: the unmodified `2.6.8` artifact remains in `~/.m2` untouched after all iterations, so reverting VTS's pom to `2.6.8` always gives a clean baseline build.

## Risks & Mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| Exception types from `getAsFloat()` differ across Gson versions | Low | Caught list covers all three documented cases for Gson 2.x. Behavior change would surface as a test failure in `ParamUtilsTest`. |
| VTS caller passes non-standard JsonArray content that Gson's `fromJson` tolerated but manual walk rejects | Low | Both paths ultimately call `JsonPrimitive.getAsFloat()`; Gson's reflective deserializer doesn't accept inputs the manual walk would reject. |
| Patched jar diverges from shaded `com.google` relocation behavior | None | Shading happens at VTS `connector-milvus` packaging time, not inside the SDK jar. Patched SDK classes are shaded identically to the upstream 2.6.8 classes. |
| Forgetting to rebuild after iteration | Low | Step 4 of the handoff always includes the explicit build command. |
| Accidentally committing VTS pom version bump | Medium | Handoff explicitly calls out the rollback command; the pom change is intentionally left as a working-tree modification. |

## Acceptance Criteria

Iteration 1 is done when:

1. `~/work/milvus-sdk-java-vts/` exists with the `vts/float-vector-fast-path` branch containing the version bump + code change commits.
2. Both patched artifacts are present in `~/.m2/repository/io/milvus/`:
   - `milvus-sdk-java/2.6.8-vts.1/milvus-sdk-java-2.6.8-vts.1.jar` (the core module — this is where the patched `ParamUtils` class lives)
   - `milvus-sdk-java-bulkwriter/2.6.8-vts.1/milvus-sdk-java-bulkwriter-2.6.8-vts.1.jar`
3. `seatunnel-connectors-v2/connector-milvus/pom.xml` has the two version bumps applied (uncommitted).
4. VTS builds successfully with `mvnd clean package -pl :seatunnel-dist -am -D"skip.ui"=true -Dmaven.test.skip=true -Prelease`.
5. Handoff summary has been delivered to the user.

Benchmarking is the user's responsibility and is out of scope for the implementation itself. The user will run the patched build in their test environment and compare against the `2.6.8` baseline.
