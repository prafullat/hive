/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package org.apache.hadoop.hive.metastore.api;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import org.apache.log4j.Logger;

import org.apache.thrift.*;
import org.apache.thrift.meta_data.*;
import org.apache.thrift.protocol.*;

public class Index implements TBase, java.io.Serializable, Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("Index");
  private static final TField INDEX_NAME_FIELD_DESC = new TField("indexName", TType.STRING, (short)1);
  private static final TField INDEX_TYPE_FIELD_DESC = new TField("indexType", TType.I32, (short)2);
  private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)3);
  private static final TField DB_NAME_FIELD_DESC = new TField("dbName", TType.STRING, (short)4);
  private static final TField COL_NAMES_FIELD_DESC = new TField("colNames", TType.LIST, (short)5);
  private static final TField PART_NAME_FIELD_DESC = new TField("partName", TType.STRING, (short)6);

  private String indexName;
  public static final int INDEXNAME = 1;
  private String indexType;
  public static final int INDEXTYPE = 2;
  private String tableName;
  public static final int TABLENAME = 3;
  private String dbName;
  public static final int DBNAME = 4;
  private List<String> colNames;
  public static final int COLNAMES = 5;
  private String partName;
  public static final int PARTNAME = 6;

  private final Isset __isset = new Isset();
  private static final class Isset implements java.io.Serializable {
    public boolean indexType = false;
  }

  public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
    put(INDEXNAME, new FieldMetaData("indexName", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(INDEXTYPE, new FieldMetaData("indexType", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(DBNAME, new FieldMetaData("dbName", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(COLNAMES, new FieldMetaData("colNames", TFieldRequirementType.DEFAULT, 
        new ListMetaData(TType.LIST, 
            new FieldValueMetaData(TType.STRING))));
    put(PARTNAME, new FieldMetaData("partName", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
  }});

  static {
    FieldMetaData.addStructMetaDataMap(Index.class, metaDataMap);
  }

  public Index() {
  }

  public Index(
    String indexName,
    String indexType,
    String tableName,
    String dbName,
    List<String> colNames,
    String partName)
  {
    this();
    this.indexName = indexName;
    this.indexType = indexType;
    this.__isset.indexType = true;
    this.tableName = tableName;
    this.dbName = dbName;
    this.colNames = colNames;
    this.partName = partName;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Index(Index other) {
    if (other.isSetIndexName()) {
      this.indexName = other.indexName;
    }
    __isset.indexType = other.__isset.indexType;
    this.indexType = other.indexType;
    if (other.isSetTableName()) {
      this.tableName = other.tableName;
    }
    if (other.isSetDbName()) {
      this.dbName = other.dbName;
    }
    if (other.isSetColNames()) {
      List<String> __this__colNames = new ArrayList<String>();
      for (String other_element : other.colNames) {
        __this__colNames.add(other_element);
      }
      this.colNames = __this__colNames;
    }
    if (other.isSetPartName()) {
      this.partName = other.partName;
    }
  }

  @Override
  public Index clone() {
    return new Index(this);
  }

  public String getIndexName() {
    return this.indexName;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  public void unsetIndexName() {
    this.indexName = null;
  }

  // Returns true if field indexName is set (has been asigned a value) and false otherwise
  public boolean isSetIndexName() {
    return this.indexName != null;
  }

  public String getIndexType() {
    return this.indexType;
  }

  public void setIndexType(String indexType) {
    this.indexType = indexType;
    this.__isset.indexType = true;
  }

  public void unsetIndexType() {
    this.__isset.indexType = false;
  }

  // Returns true if field indexType is set (has been asigned a value) and false otherwise
  public boolean isSetIndexType() {
    return this.__isset.indexType;
  }

  public String getTableName() {
    return this.tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void unsetTableName() {
    this.tableName = null;
  }

  // Returns true if field tableName is set (has been asigned a value) and false otherwise
  public boolean isSetTableName() {
    return this.tableName != null;
  }

  public String getDbName() {
    return this.dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public void unsetDbName() {
    this.dbName = null;
  }

  // Returns true if field dbName is set (has been asigned a value) and false otherwise
  public boolean isSetDbName() {
    return this.dbName != null;
  }

  public int getColNamesSize() {
    return (this.colNames == null) ? 0 : this.colNames.size();
  }

  public java.util.Iterator<String> getColNamesIterator() {
    return (this.colNames == null) ? null : this.colNames.iterator();
  }

  public void addToColNames(String elem) {
    if (this.colNames == null) {
      this.colNames = new ArrayList<String>();
    }
    this.colNames.add(elem);
  }

  public List<String> getColNames() {
    return this.colNames;
  }

  public void setColNames(List<String> colNames) {
    this.colNames = colNames;
  }

  public void unsetColNames() {
    this.colNames = null;
  }

  // Returns true if field colNames is set (has been asigned a value) and false otherwise
  public boolean isSetColNames() {
    return this.colNames != null;
  }

  public String getPartName() {
    return this.partName;
  }

  public void setPartName(String partName) {
    this.partName = partName;
  }

  public void unsetPartName() {
    this.partName = null;
  }

  // Returns true if field partName is set (has been asigned a value) and false otherwise
  public boolean isSetPartName() {
    return this.partName != null;
  }

  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case INDEXNAME:
      if (value == null) {
        unsetIndexName();
      } else {
        setIndexName((String)value);
      }
      break;

    case INDEXTYPE:
      if (value == null) {
        unsetIndexType();
      } else {
        setIndexType((String)value);
      }
      break;

    case TABLENAME:
      if (value == null) {
        unsetTableName();
      } else {
        setTableName((String)value);
      }
      break;

    case DBNAME:
      if (value == null) {
        unsetDbName();
      } else {
        setDbName((String)value);
      }
      break;

    case COLNAMES:
      if (value == null) {
        unsetColNames();
      } else {
        setColNames((List<String>)value);
      }
      break;

    case PARTNAME:
      if (value == null) {
        unsetPartName();
      } else {
        setPartName((String)value);
      }
      break;

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case INDEXNAME:
      return getIndexName();

    case INDEXTYPE:
      return new Integer(getIndexType());

    case TABLENAME:
      return getTableName();

    case DBNAME:
      return getDbName();

    case COLNAMES:
      return getColNames();

    case PARTNAME:
      return getPartName();

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case INDEXNAME:
      return isSetIndexName();
    case INDEXTYPE:
      return isSetIndexType();
    case TABLENAME:
      return isSetTableName();
    case DBNAME:
      return isSetDbName();
    case COLNAMES:
      return isSetColNames();
    case PARTNAME:
      return isSetPartName();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Index)
      return this.equals((Index)that);
    return false;
  }

  public boolean equals(Index that) {
    if (that == null)
      return false;

    boolean this_present_indexName = true && this.isSetIndexName();
    boolean that_present_indexName = true && that.isSetIndexName();
    if (this_present_indexName || that_present_indexName) {
      if (!(this_present_indexName && that_present_indexName))
        return false;
      if (!this.indexName.equals(that.indexName))
        return false;
    }

    boolean this_present_indexType = true;
    boolean that_present_indexType = true;
    if (this_present_indexType || that_present_indexType) {
      if (!(this_present_indexType && that_present_indexType))
        return false;
      if (this.indexType != that.indexType)
        return false;
    }

    boolean this_present_tableName = true && this.isSetTableName();
    boolean that_present_tableName = true && that.isSetTableName();
    if (this_present_tableName || that_present_tableName) {
      if (!(this_present_tableName && that_present_tableName))
        return false;
      if (!this.tableName.equals(that.tableName))
        return false;
    }

    boolean this_present_dbName = true && this.isSetDbName();
    boolean that_present_dbName = true && that.isSetDbName();
    if (this_present_dbName || that_present_dbName) {
      if (!(this_present_dbName && that_present_dbName))
        return false;
      if (!this.dbName.equals(that.dbName))
        return false;
    }

    boolean this_present_colNames = true && this.isSetColNames();
    boolean that_present_colNames = true && that.isSetColNames();
    if (this_present_colNames || that_present_colNames) {
      if (!(this_present_colNames && that_present_colNames))
        return false;
      if (!this.colNames.equals(that.colNames))
        return false;
    }

    boolean this_present_partName = true && this.isSetPartName();
    boolean that_present_partName = true && that.isSetPartName();
    if (this_present_partName || that_present_partName) {
      if (!(this_present_partName && that_present_partName))
        return false;
      if (!this.partName.equals(that.partName))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public void read(TProtocol iprot) throws TException {
    TField field;
    iprot.readStructBegin();
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == TType.STOP) { 
        break;
      }
      switch (field.id)
      {
        case INDEXNAME:
          if (field.type == TType.STRING) {
            this.indexName = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case INDEXTYPE:
          if (field.type == TType.I32) {
            this.indexType = iprot.readString();
            this.__isset.indexType = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case TABLENAME:
          if (field.type == TType.STRING) {
            this.tableName = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case DBNAME:
          if (field.type == TType.STRING) {
            this.dbName = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case COLNAMES:
          if (field.type == TType.LIST) {
            {
              TList _list44 = iprot.readListBegin();
              this.colNames = new ArrayList<String>(_list44.size);
              for (int _i45 = 0; _i45 < _list44.size; ++_i45)
              {
                String _elem46;
                _elem46 = iprot.readString();
                this.colNames.add(_elem46);
              }
              iprot.readListEnd();
            }
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case PARTNAME:
          if (field.type == TType.STRING) {
            this.partName = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          TProtocolUtil.skip(iprot, field.type);
          break;
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();

    validate();
  }

  public void write(TProtocol oprot) throws TException {
    validate();

    oprot.writeStructBegin(STRUCT_DESC);
    if (this.indexName != null) {
      oprot.writeFieldBegin(INDEX_NAME_FIELD_DESC);
      oprot.writeString(this.indexName);
      oprot.writeFieldEnd();
    }
    if (this.tableName != null) {
      oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
      oprot.writeString(this.tableName);
      oprot.writeFieldEnd();
    }
    if (this.dbName != null) {
      oprot.writeFieldBegin(DB_NAME_FIELD_DESC);
      oprot.writeString(this.dbName);
      oprot.writeFieldEnd();
    }
    if (this.colNames != null) {
      oprot.writeFieldBegin(COL_NAMES_FIELD_DESC);
      {
        oprot.writeListBegin(new TList(TType.STRING, this.colNames.size()));
        for (String _iter47 : this.colNames)        {
          oprot.writeString(_iter47);
        }
        oprot.writeListEnd();
      }
      oprot.writeFieldEnd();
    }
    if (this.partName != null) {
      oprot.writeFieldBegin(PART_NAME_FIELD_DESC);
      oprot.writeString(this.partName);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Index(");
    boolean first = true;

    sb.append("indexName:");
    if (this.indexName == null) {
      sb.append("null");
    } else {
      sb.append(this.indexName);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("indexType:");
    sb.append(this.indexType);
    first = false;
    if (!first) sb.append(", ");
    sb.append("tableName:");
    if (this.tableName == null) {
      sb.append("null");
    } else {
      sb.append(this.tableName);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("dbName:");
    if (this.dbName == null) {
      sb.append("null");
    } else {
      sb.append(this.dbName);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("colNames:");
    if (this.colNames == null) {
      sb.append("null");
    } else {
      sb.append(this.colNames);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("partName:");
    if (this.partName == null) {
      sb.append("null");
    } else {
      sb.append(this.partName);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
    // check that fields of type enum have valid values
  }

}

