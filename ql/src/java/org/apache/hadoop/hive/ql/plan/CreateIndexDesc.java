/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.List;

/**
 * create index descriptor
 */
public class CreateIndexDesc extends DDLDesc implements Serializable {

  private static final long serialVersionUID = 1L;
  String tableName;
  String indexName;
  List<String> indexedCols;
  String inputFormat;
  String outputFormat;
  String serde;
  String indexType;

  public CreateIndexDesc() {
    super();
  }

  public CreateIndexDesc(String tableName, String indexName,
      List<String> indexedCols,String inputFormat, String outputFormat, String serde, String indexType) {
    super();
    this.tableName = tableName;
    this.indexName = indexName;
    this.indexedCols = indexedCols;
    this.indexType = indexType;
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.serde = serde;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getIndexName() {
    return indexName;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  public List<String> getIndexedCols() {
    return indexedCols;
  }

  public void setIndexedCols(List<String> indexedCols) {
    this.indexedCols = indexedCols;
  }
  
  public String getInputFormat() {
    return inputFormat;
  }

  public void setInputFormat(String inputFormat) {
    this.inputFormat = inputFormat;
  }

  public String getOutputFormat() {
    return outputFormat;
  }

  public void setOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
  }

  public String getSerde() {
    return serde;
  }

  public void setSerde(String serde) {
    this.serde = serde;
  }
  
  public String getIndexType() {
    return indexType;
  }

  public void setIndexType(String indexType) {
    this.indexType = indexType;
  }

}
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.List;

/**
 * create index descriptor
 */
public class CreateIndexDesc extends DDLDesc implements Serializable {

  private static final long serialVersionUID = 1L;
  String tableName;
  String indexName;
  List<String> indexedCols;
  String inputFormat;
  String outputFormat;
  String serde;
  String indexType;

  public CreateIndexDesc() {
    super();
  }

  public CreateIndexDesc(String tableName, String indexName,
      List<String> indexedCols,String inputFormat, String outputFormat, String serde, String indexType) {
    super();
    this.tableName = tableName;
    this.indexName = indexName;
    this.indexedCols = indexedCols;
    this.indexType = indexType;
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.serde = serde;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getIndexName() {
    return indexName;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  public List<String> getIndexedCols() {
    return indexedCols;
  }

  public void setIndexedCols(List<String> indexedCols) {
    this.indexedCols = indexedCols;
  }
  
  public String getInputFormat() {
    return inputFormat;
  }

  public void setInputFormat(String inputFormat) {
    this.inputFormat = inputFormat;
  }

  public String getOutputFormat() {
    return outputFormat;
  }

  public void setOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
  }

  public String getSerde() {
    return serde;
  }

  public void setSerde(String serde) {
    this.serde = serde;
  }
  
  public String getIndexType() {
    return indexType;
  }

  public void setIndexType(String indexType) {
    this.indexType = indexType;
  }

}
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.List;

/**
 * create index descriptor
 */
public class CreateIndexDesc extends DDLDesc implements Serializable {

  private static final long serialVersionUID = 1L;
  String tableName;
  String indexName;
  List<String> indexedCols;
  String inputFormat;
  String outputFormat;
  String serde;
  String indexType;

  public CreateIndexDesc() {
    super();
  }

  public CreateIndexDesc(String tableName, String indexName,
      List<String> indexedCols,String inputFormat, String outputFormat, String serde, String indexType) {
    super();
    this.tableName = tableName;
    this.indexName = indexName;
    this.indexedCols = indexedCols;
    this.indexType = indexType;
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.serde = serde;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getIndexName() {
    return indexName;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  public List<String> getIndexedCols() {
    return indexedCols;
  }

  public void setIndexedCols(List<String> indexedCols) {
    this.indexedCols = indexedCols;
  }
  
  public String getInputFormat() {
    return inputFormat;
  }

  public void setInputFormat(String inputFormat) {
    this.inputFormat = inputFormat;
  }

  public String getOutputFormat() {
    return outputFormat;
  }

  public void setOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
  }

  public String getSerde() {
    return serde;
  }

  public void setSerde(String serde) {
    this.serde = serde;
  }
  
  public String getIndexType() {
    return indexType;
  }

  public void setIndexType(String indexType) {
    this.indexType = indexType;
  }

}
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.List;

/**
 * create index descriptor
 */
public class CreateIndexDesc extends DDLDesc implements Serializable {

  private static final long serialVersionUID = 1L;
  String tableName;
  String indexName;
  List<String> indexedCols;
  String inputFormat;
  String outputFormat;
  String serde;
  String indexType;

  public CreateIndexDesc() {
    super();
  }

  public CreateIndexDesc(String tableName, String indexName,
      List<String> indexedCols,String inputFormat, String outputFormat, String serde, String indexType) {
    super();
    this.tableName = tableName;
    this.indexName = indexName;
    this.indexedCols = indexedCols;
    this.indexType = indexType;
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.serde = serde;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getIndexName() {
    return indexName;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  public List<String> getIndexedCols() {
    return indexedCols;
  }

  public void setIndexedCols(List<String> indexedCols) {
    this.indexedCols = indexedCols;
  }
  
  public String getInputFormat() {
    return inputFormat;
  }

  public void setInputFormat(String inputFormat) {
    this.inputFormat = inputFormat;
  }

  public String getOutputFormat() {
    return outputFormat;
  }

  public void setOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
  }

  public String getSerde() {
    return serde;
  }

  public void setSerde(String serde) {
    this.serde = serde;
  }
  
  public String getIndexType() {
    return indexType;
  }

  public void setIndexType(String indexType) {
    this.indexType = indexType;
  }

}