import dotenv from "dotenv";
dotenv.config();
import mysql from "mysql2/promise";
import mssql from "mssql";

async function setupConnections() {
  try {
    const connectionToWebDiskon = await mysql.createConnection({
      host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASS,
      database: process.env.DB_NAME_DISKON,
    });

    console.log("Connected to WebDiskon database!");

    const connectionToSimpi = await mysql.createConnection({
      host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASS,
      database: process.env.DB_NAME,
    });

    console.log("Connected to Simpi database!");

    const connectionToSqlServer = await mssql.connect({
      user: process.env.SQL_DB_USER,
      password: process.env.SQL_DB_PASS,
      database: process.env.SQL_DB_NAME_TEST,
      server: process.env.SQL_HOST,
    });
    console.log(connectionToSqlServer, "connectionToSqlServer");
    return { connectionToWebDiskon, connectionToSimpi, connectionToSqlServer };
  } catch (err) {
    console.error("Error connecting to databases:", err);
    throw err;
  }
}

export default setupConnections;
