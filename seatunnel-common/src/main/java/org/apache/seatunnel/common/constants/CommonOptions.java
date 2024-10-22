package org.apache.seatunnel.common.constants;

import lombok.Getter;

@Getter
public enum CommonOptions {
    JSON("Json"),
    METADATA("Metadata");

    private final String name;

    CommonOptions(String name) {
        this.name = name;
    }
}
