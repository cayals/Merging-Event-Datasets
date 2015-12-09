package com.micron.data;

import java.io.IOException;
import java.io.*;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKey implements WritableComparable<CompositeKey> {
 
private String first_key;
private String second_key;
private String third_key;
 
public CompositeKey() {
}
 
public CompositeKey(String first_key, String second_key, String third_key) {
 
this.first_key = first_key;
this.second_key = second_key;
this.third_key = third_key;
}
 
@Override
public String toString() {
 
return (new StringBuilder()).append(first_key).append(',').append(second_key).append(',').append(third_key).toString();
}
 
@Override
public void readFields(DataInput in) throws IOException {
 
first_key = WritableUtils.readString(in);
second_key = WritableUtils.readString(in);
third_key = WritableUtils.readString(in);
}
 
@Override
public void write(DataOutput out) throws IOException {
 
WritableUtils.writeString(out, first_key);
WritableUtils.writeString(out, second_key);
WritableUtils.writeString(out, third_key);
}
 
@Override
public int compareTo(CompositeKey o) {
 
int result = first_key.compareTo(o.first_key);
if (0 == result) {
result = second_key.compareTo(o.second_key);
}
if (0 == result) {
result = third_key.compareTo(o.third_key);
}
return result;
}
 
/**
* Gets the first_key.
*
* @return first_key.
*/
public String getfirst_key() {
 
return first_key;
}
 
public void setfirst_key(String first_key) {
 
this.first_key = first_key;
}
 
/**
* Gets the second_key.
*
* @return second_key
*/
public String getsecond_key() {
 
return second_key;
}

/**
* Gets the second_key.
*
* @return second_key
*/
public String getthird_key() {
 
return third_key;
}

public void setthird_key(String third_key) {
 
this.third_key = third_key;
}

 public static int compare(int a, int b) {
    return (a < b ? -1 : (a == b ? 0 : 1));
}


}