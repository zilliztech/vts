package org.apache.seatunnel.common.constants;

import lombok.Getter;

@Getter
public enum CommonOptions {
    JSON("Json"),
    METADATA("Metadata"),
    ELEMENT_TYPE("ElementType"),
    MAX_CAPACITY("MaxCapacity"),
    MAX_LENGTH("MaxLength"),;

    private final String name;

    CommonOptions(String name) {
        this.name = name;
    }
}
