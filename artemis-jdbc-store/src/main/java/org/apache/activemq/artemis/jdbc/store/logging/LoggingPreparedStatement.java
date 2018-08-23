/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.jdbc.store.logging;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import org.jboss.logging.Logger;

public class LoggingPreparedStatement implements PreparedStatement {

   final PreparedStatement preparedStatement;

   final String preparedStatementID;

   Logger logger;

   Logger.Level level = Logger.Level.INFO;

   public LoggingPreparedStatement(PreparedStatement preparedStatement, Logger logger) {
      this.preparedStatement = preparedStatement;
      this.logger = logger;
      this.preparedStatementID = LoggingUtil.getID(preparedStatement);
   }

   PreparedStatement getPreparedStatement() {
      return preparedStatement;
   }

   @Override
   public ResultSet executeQuery() throws SQLException {
      ResultSet rs = preparedStatement.executeQuery();
      logger.logf(level, "%s.executeQuery() = %s", preparedStatementID, LoggingUtil.getID(rs));
      return rs;
   }

   @Override
   public int executeUpdate() throws SQLException {
      return preparedStatement.executeUpdate();
   }

   @Override
   public void setNull(int parameterIndex, int sqlType) throws SQLException {
      logger.logf(level, "%s.setNull(%d, %d)", preparedStatementID, parameterIndex, sqlType);
      preparedStatement.setNull(parameterIndex, sqlType);
   }

   @Override
   public void setBoolean(int parameterIndex, boolean x) throws SQLException {
      logger.logf(level, "%s.setBoolean(%d, %s)", preparedStatementID, parameterIndex, x);
      preparedStatement.setBoolean(parameterIndex, x);
   }

   @Override
   public void setByte(int parameterIndex, byte x) throws SQLException {
      logger.logf(level, "%s.setByte(%d, %s)", preparedStatementID, parameterIndex, x);
      preparedStatement.setByte(parameterIndex, x);
   }

   @Override
   public void setShort(int parameterIndex, short x) throws SQLException {
      logger.logf(level, "%s.setShort(%d, %d)", preparedStatementID, parameterIndex, x);
      preparedStatement.setShort(parameterIndex, x);
   }

   @Override
   public void setInt(int parameterIndex, int x) throws SQLException {
      logger.logf(level, "%s.setInt(%d, %d)", preparedStatementID, parameterIndex, x);
      preparedStatement.setInt(parameterIndex, x);
   }

   @Override
   public void setLong(int parameterIndex, long x) throws SQLException {
      logger.logf(level, "%s.setLong(%d, %s)", preparedStatementID, parameterIndex, x);
      preparedStatement.setLong(parameterIndex, x);
   }

   @Override
   public void setFloat(int parameterIndex, float x) throws SQLException {
      logger.logf(level, "%s.setFloat(%d, %f)", preparedStatementID, parameterIndex, x);
      preparedStatement.setFloat(parameterIndex, x);
   }

   @Override
   public void setDouble(int parameterIndex, double x) throws SQLException {
      logger.logf(level, "%s.setDouble(%d, %d)", preparedStatementID, parameterIndex, x);
      preparedStatement.setDouble(parameterIndex, x);
   }

   @Override
   public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
      logger.logf(level, "%s.setBigDecimal(%d, %s)", preparedStatementID, parameterIndex, x);
      preparedStatement.setBigDecimal(parameterIndex, x);
   }

   @Override
   public void setString(int parameterIndex, String x) throws SQLException {
      logger.logf(level, "%s.setString(%d, %s)", preparedStatementID, parameterIndex, x);
      preparedStatement.setString(parameterIndex, x);
   }

   @Override
   public void setBytes(int parameterIndex, byte[] x) throws SQLException {
      logger.logf(level, "%s.setBytes(%d, %s)", preparedStatementID, parameterIndex, x);
      preparedStatement.setBytes(parameterIndex, x);
   }

   @Override
   public void setDate(int parameterIndex, Date x) throws SQLException {
      logger.logf(level, "%s.setDate(%d, %s)", preparedStatementID, parameterIndex, x);
      preparedStatement.setDate(parameterIndex, x);
   }

   @Override
   public void setTime(int parameterIndex, Time x) throws SQLException {
      logger.logf(level, "%s.setTime(%d, %s)", preparedStatementID, parameterIndex, x);
      preparedStatement.setTime(parameterIndex, x);
   }

   @Override
   public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
      logger.logf(level, "%s.setTimestamp(%d, %s)", preparedStatementID, parameterIndex, x);
      preparedStatement.setTimestamp(parameterIndex, x);
   }

   @Override
   public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
      logger.logf(level, "%s.setAsciiStream(%d, %s, %d)", preparedStatementID, parameterIndex, x, length);
      preparedStatement.setAsciiStream(parameterIndex, x, length);
   }

   @Override
   public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
      logger.logf(level, "%s.setUnicodeStream(%d, %s, %d)", preparedStatementID, parameterIndex, x, length);
      preparedStatement.setUnicodeStream(parameterIndex, x, length);
   }

   @Override
   public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
      logger.logf(level, "%s.setBinaryStream(%d, %s, %d)", preparedStatementID, parameterIndex, x, length);
      preparedStatement.setBinaryStream(parameterIndex, x, length);
   }

   @Override
   public void clearParameters() throws SQLException {
      preparedStatement.clearParameters();
   }

   @Override
   public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
      logger.logf(level, "%s.setObject(%d, %s, %d)", preparedStatementID, parameterIndex, x, targetSqlType);
      preparedStatement.setObject(parameterIndex, x, targetSqlType);
   }

   @Override
   public void setObject(int parameterIndex, Object x) throws SQLException {
      logger.logf(level, "%s.setObject(%d, %s)", preparedStatementID, parameterIndex, x);
      preparedStatement.setObject(parameterIndex, x);
   }

   @Override
   public boolean execute() throws SQLException {
      return preparedStatement.execute();
   }

   @Override
   public void addBatch() throws SQLException {
      preparedStatement.addBatch();
   }

   @Override
   public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
      logger.logf(level, "%s.setCharacterStream(%d, %s, %d)", preparedStatementID, parameterIndex, reader, length);
      preparedStatement.setCharacterStream(parameterIndex, reader, length);
   }

   @Override
   public void setRef(int parameterIndex, Ref x) throws SQLException {
      logger.logf(level, "%s.setRef(%d, %s)", preparedStatementID, parameterIndex, x);
      preparedStatement.setRef(parameterIndex, x);
   }

   @Override
   public void setBlob(int parameterIndex, Blob x) throws SQLException {
      logger.logf(level, "%s.setBlob(%d, %s)", preparedStatementID, parameterIndex, x);
      preparedStatement.setBlob(parameterIndex, x);
   }

   @Override
   public void setClob(int parameterIndex, Clob x) throws SQLException {
      logger.logf(level, "%s.setClob(%d, %x)", preparedStatementID, parameterIndex, x);
      preparedStatement.setClob(parameterIndex, x);
   }

   @Override
   public void setArray(int parameterIndex, Array x) throws SQLException {
      logger.logf(level, "%s.setArray(%d, %s)", preparedStatementID, parameterIndex, x);
      preparedStatement.setArray(parameterIndex, x);
   }

   @Override
   public ResultSetMetaData getMetaData() throws SQLException {
      return preparedStatement.getMetaData();
   }

   @Override
   public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
      logger.logf(level, "%s.setDate(%d, %s, %s)", preparedStatementID, parameterIndex, x, cal);
      preparedStatement.setDate(parameterIndex, x, cal);
   }

   @Override
   public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
      logger.logf(level, "%s.setTime(%d, %s, %s)", preparedStatementID, parameterIndex, x, cal);
      preparedStatement.setTime(parameterIndex, x, cal);
   }

   @Override
   public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
      logger.logf(level, "%s.setTimestamp(%d, %s, %s)", preparedStatementID, parameterIndex, x, cal);
      preparedStatement.setTimestamp(parameterIndex, x, cal);
   }

   @Override
   public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
      logger.logf(level, "%s.setNull(%d, %d, %s)", preparedStatementID, parameterIndex, sqlType, typeName);
      preparedStatement.setNull(parameterIndex, sqlType, typeName);
   }

   @Override
   public void setURL(int parameterIndex, URL x) throws SQLException {
      logger.logf(level, "%s.setURL(%d, %s)", preparedStatementID, parameterIndex, x);
      preparedStatement.setURL(parameterIndex, x);
   }

   @Override
   public ParameterMetaData getParameterMetaData() throws SQLException {
      return preparedStatement.getParameterMetaData();
   }

   @Override
   public void setRowId(int parameterIndex, RowId x) throws SQLException {
      logger.logf(level, "%s.setRowId(%d, %s)", preparedStatementID, parameterIndex, x);
      preparedStatement.setRowId(parameterIndex, x);
   }

   @Override
   public void setNString(int parameterIndex, String value) throws SQLException {
      logger.logf(level, "%s.setNString(%d, %s)", preparedStatementID, parameterIndex, value);
      preparedStatement.setNString(parameterIndex, value);
   }

   @Override
   public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
      logger.logf(level, "%s.setNCharacterStream(%d, %s, %d)", preparedStatementID, parameterIndex, value, length);
      preparedStatement.setNCharacterStream(parameterIndex, value, length);
   }

   @Override
   public void setNClob(int parameterIndex, NClob value) throws SQLException {
      logger.logf(level, "%s.setNClob(%d, %s)", preparedStatementID, parameterIndex, value);
      preparedStatement.setNClob(parameterIndex, value);
   }

   @Override
   public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
      logger.logf(level, "%s.setClob(%d, %s, %s)", preparedStatementID, parameterIndex, reader, length);
      preparedStatement.setClob(parameterIndex, reader, length);
   }

   @Override
   public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
      logger.logf(level, "%s.setBlob(%d, %s, %d)", preparedStatementID, parameterIndex, inputStream, length);
      preparedStatement.setBlob(parameterIndex, inputStream, length);
   }

   @Override
   public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
      logger.logf(level, "%s.setNClob(%d, %s, %d)", preparedStatementID, parameterIndex, reader, length);
      preparedStatement.setNClob(parameterIndex, reader, length);
   }

   @Override
   public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
      logger.logf(level, "%s.setSQLXML(%d, %s)", preparedStatementID, parameterIndex, xmlObject);
      preparedStatement.setSQLXML(parameterIndex, xmlObject);
   }

   @Override
   public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
      logger.logf(level, "%s.setNull(%d, %d)", preparedStatementID, parameterIndex, x);
      preparedStatement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
   }

   @Override
   public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
      logger.logf(level, "%s.setNull(%d, %d)", preparedStatementID, parameterIndex, x);
      preparedStatement.setAsciiStream(parameterIndex, x, length);
   }

   @Override
   public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
      logger.logf(level, "%s.setNull(%d, %d)", preparedStatementID, parameterIndex, x);
      preparedStatement.setBinaryStream(parameterIndex, x, length);
   }

   @Override
   public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
      logger.logf(level, "%s.setCharacterStream(%d, %s, %d)", preparedStatementID, parameterIndex, reader, length);
      preparedStatement.setCharacterStream(parameterIndex, reader, length);
   }

   @Override
   public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
      logger.logf(level, "%s.setAsciiStream(%d, %d)", preparedStatementID, parameterIndex, x);
      preparedStatement.setAsciiStream(parameterIndex, x);
   }

   @Override
   public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
      logger.logf(level, "%s.setBinaryStream(%d, %s)", preparedStatementID, parameterIndex, x);
      preparedStatement.setBinaryStream(parameterIndex, x);
   }

   @Override
   public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
      logger.logf(level, "%s.setCharacterStream(%d, %s)", preparedStatementID, parameterIndex, reader);
      preparedStatement.setCharacterStream(parameterIndex, reader);
   }

   @Override
   public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
      logger.logf(level, "%s.setNCharacterStream(%d, %s)", preparedStatementID, parameterIndex, value);
      preparedStatement.setNCharacterStream(parameterIndex, value);
   }

   @Override
   public void setClob(int parameterIndex, Reader reader) throws SQLException {
      logger.logf(level, "%s.setClob(%d, %s)", preparedStatementID, parameterIndex, reader);
      preparedStatement.setClob(parameterIndex, reader);
   }

   @Override
   public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
      logger.logf(level, "%s.setBlob(%d, %s)", preparedStatementID, parameterIndex, inputStream);
      preparedStatement.setBlob(parameterIndex, inputStream);
   }

   @Override
   public void setNClob(int parameterIndex, Reader reader) throws SQLException {
      logger.logf(level, "%s.setNClob(%d, %s)", preparedStatementID, parameterIndex, reader);
      preparedStatement.setNClob(parameterIndex, reader);
   }

   @Override
   public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
      logger.logf(level, "%s.setObject(%d, %s, %s, %d)", preparedStatementID, parameterIndex, x, targetSqlType, scaleOrLength);
      preparedStatement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
   }

   @Override
   public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
      logger.logf(level, "%s.setObject(%d, %s, %d)", preparedStatementID, parameterIndex, x, targetSqlType);
      preparedStatement.setObject(parameterIndex, x, targetSqlType);
   }

   @Override
   public long executeLargeUpdate() throws SQLException {
      return preparedStatement.executeLargeUpdate();
   }

   @Override
   public ResultSet executeQuery(String sql) throws SQLException {
      return preparedStatement.executeQuery(sql);
   }

   @Override
   public int executeUpdate(String sql) throws SQLException {
      return preparedStatement.executeUpdate(sql);
   }

   @Override
   public void close() throws SQLException {
      preparedStatement.close();
   }

   @Override
   public int getMaxFieldSize() throws SQLException {
      return preparedStatement.getMaxFieldSize();
   }

   @Override
   public void setMaxFieldSize(int max) throws SQLException {
      logger.logf(level, "%s.setMaxFieldSize(%d)", preparedStatementID, max);
      preparedStatement.setMaxFieldSize(max);
   }

   @Override
   public int getMaxRows() throws SQLException {
      return preparedStatement.getMaxRows();
   }

   @Override
   public void setMaxRows(int max) throws SQLException {
      preparedStatement.setMaxRows(max);
   }

   @Override
   public void setEscapeProcessing(boolean enable) throws SQLException {
      logger.logf(level, "%s.setEscapeProcessing(%s)", preparedStatementID, enable);
      preparedStatement.setEscapeProcessing(enable);
   }

   @Override
   public int getQueryTimeout() throws SQLException {
      return preparedStatement.getQueryTimeout();
   }

   @Override
   public void setQueryTimeout(int seconds) throws SQLException {
      logger.logf(level, "%s.setQueryTimeout(%d)", preparedStatementID, seconds);
      preparedStatement.setQueryTimeout(seconds);
   }

   @Override
   public void cancel() throws SQLException {
      preparedStatement.cancel();
   }

   @Override
   public SQLWarning getWarnings() throws SQLException {
      return preparedStatement.getWarnings();
   }

   @Override
   public void clearWarnings() throws SQLException {
      preparedStatement.clearWarnings();
   }

   @Override
   public void setCursorName(String name) throws SQLException {
      logger.logf(level, "%s.setCursorName(%s)", preparedStatementID, name);
      preparedStatement.setCursorName(name);
   }

   @Override
   public boolean execute(String sql) throws SQLException {
      return preparedStatement.execute(sql);
   }

   @Override
   public ResultSet getResultSet() throws SQLException {
      return preparedStatement.getResultSet();
   }

   @Override
   public int getUpdateCount() throws SQLException {
      return preparedStatement.getUpdateCount();
   }

   @Override
   public boolean getMoreResults() throws SQLException {
      return preparedStatement.getMoreResults();
   }

   @Override
   public void setFetchDirection(int direction) throws SQLException {
      preparedStatement.setFetchDirection(direction);
   }

   @Override
   public int getFetchDirection() throws SQLException {
      return preparedStatement.getFetchDirection();
   }

   @Override
   public void setFetchSize(int rows) throws SQLException {
      logger.logf(level, "%s.setFetchSize(%d)", preparedStatementID, rows);
      preparedStatement.setFetchSize(rows);
   }

   @Override
   public int getFetchSize() throws SQLException {
      return preparedStatement.getFetchSize();
   }

   @Override
   public int getResultSetConcurrency() throws SQLException {
      return preparedStatement.getResultSetConcurrency();
   }

   @Override
   public int getResultSetType() throws SQLException {
      return preparedStatement.getResultSetType();
   }

   @Override
   public void addBatch(String sql) throws SQLException {
      preparedStatement.addBatch(sql);
   }

   @Override
   public void clearBatch() throws SQLException {
      preparedStatement.clearBatch();
   }

   @Override
   public int[] executeBatch() throws SQLException {
      return preparedStatement.executeBatch();
   }

   @Override
   public Connection getConnection() throws SQLException {
      return preparedStatement.getConnection();
   }

   @Override
   public boolean getMoreResults(int current) throws SQLException {
      return preparedStatement.getMoreResults(current);
   }

   @Override
   public ResultSet getGeneratedKeys() throws SQLException {
      return preparedStatement.getGeneratedKeys();
   }

   @Override
   public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
      return preparedStatement.executeUpdate(sql, autoGeneratedKeys);
   }

   @Override
   public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
      return preparedStatement.executeUpdate(sql, columnIndexes);
   }

   @Override
   public int executeUpdate(String sql, String[] columnNames) throws SQLException {
      return preparedStatement.executeUpdate(sql, columnNames);
   }

   @Override
   public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
      return preparedStatement.execute(sql, autoGeneratedKeys);
   }

   @Override
   public boolean execute(String sql, int[] columnIndexes) throws SQLException {
      return preparedStatement.execute(sql, columnIndexes);
   }

   @Override
   public boolean execute(String sql, String[] columnNames) throws SQLException {
      return preparedStatement.execute(sql, columnNames);
   }

   @Override
   public int getResultSetHoldability() throws SQLException {
      return preparedStatement.getResultSetHoldability();
   }

   @Override
   public boolean isClosed() throws SQLException {
      return preparedStatement.isClosed();
   }

   @Override
   public void setPoolable(boolean poolable) throws SQLException {
      logger.logf(level, "%s.setPoolable(%s)", preparedStatementID, poolable);
      preparedStatement.setPoolable(poolable);
   }

   @Override
   public boolean isPoolable() throws SQLException {
      return preparedStatement.isPoolable();
   }

   @Override
   public void closeOnCompletion() throws SQLException {
      preparedStatement.closeOnCompletion();
   }

   @Override
   public boolean isCloseOnCompletion() throws SQLException {
      return preparedStatement.isCloseOnCompletion();
   }

   @Override
   public long getLargeUpdateCount() throws SQLException {
      return preparedStatement.getLargeUpdateCount();
   }

   @Override
   public void setLargeMaxRows(long max) throws SQLException {
      preparedStatement.setLargeMaxRows(max);
   }

   @Override
   public long getLargeMaxRows() throws SQLException {
      return preparedStatement.getLargeMaxRows();
   }

   @Override
   public long[] executeLargeBatch() throws SQLException {
      return preparedStatement.executeLargeBatch();
   }

   @Override
   public long executeLargeUpdate(String sql) throws SQLException {
      return preparedStatement.executeLargeUpdate(sql);
   }

   @Override
   public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
      return preparedStatement.executeLargeUpdate(sql, autoGeneratedKeys);
   }

   @Override
   public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
      return preparedStatement.executeLargeUpdate(sql, columnIndexes);
   }

   @Override
   public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
      return preparedStatement.executeLargeUpdate(sql, columnNames);
   }

   @Override
   public <T> T unwrap(Class<T> iface) throws SQLException {
      return preparedStatement.unwrap(iface);
   }

   @Override
   public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return preparedStatement.isWrapperFor(iface);
   }
}
