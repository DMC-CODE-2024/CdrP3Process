
package com.glocks.parser;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Date;
import java.util.Map;

//ETl-Class
public class InsertDbDao implements Runnable {

    static Logger logger = LogManager.getLogger(InsertDbDao.class);

    String query;
    Connection conn;
    Map<String, String> map;

    public InsertDbDao() {
    }

    public InsertDbDao(Connection conn, String query) {
        this.conn = conn;
        this.query = query;
    }

    public InsertDbDao(Connection conn, String query, Map<String, String> map) {
        this.conn = conn;
        this.query = query;
        this.map = map;
    }

    @Override
    public void run() {
        var date = new Date();
        try (Statement stmtNew = conn.createStatement()) {
            stmtNew.executeUpdate(query);
            logger.info("Query->  [" + query + " ] . at  " + date);
        } catch (Exception e) {
            logger.error("ThreadError-- {} ->-<- {}  --- [{}] . Time {}", e.getLocalizedMessage(), e, query, date);
        }
    }

}
