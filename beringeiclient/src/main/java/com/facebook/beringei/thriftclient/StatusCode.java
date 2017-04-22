package com.facebook.beringei.thriftclient;

/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */

public enum StatusCode implements org.apache.thrift.TEnum {
  OK(0),
  DONT_OWN_SHARD(1),
  KEY_MISSING(2),
  RPC_FAIL(3),
  SHARD_IN_PROGRESS(4),
  BUCKET_NOT_FINALIZED(5),
  ZIPPY_STORAGE_FAIL(6),
  MISSING_TOO_MUCH_DATA(7);

  private final int value;

  private StatusCode(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static StatusCode findByValue(int value) { 
    switch (value) {
      case 0:
        return OK;
      case 1:
        return DONT_OWN_SHARD;
      case 2:
        return KEY_MISSING;
      case 3:
        return RPC_FAIL;
      case 4:
        return SHARD_IN_PROGRESS;
      case 5:
        return BUCKET_NOT_FINALIZED;
      case 6:
        return ZIPPY_STORAGE_FAIL;
      case 7:
        return MISSING_TOO_MUCH_DATA;
      default:
        return null;
    }
  }
}
