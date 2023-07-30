import dotenv from "dotenv";
dotenv.config();
import mysql from "mysql2/promise";

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
    return { connectionToWebDiskon, connectionToSimpi };
  } catch (err) {
    console.error("Error connecting to databases:", err);
    throw err;
  }
}

export default setupConnections;
