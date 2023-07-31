import dotenv from "dotenv";
dotenv.config();
import mysql from "mysql2/promise";

async function setupConnections() {
  try {
    const poolToWebDiskon = mysql.createPool({
      host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASS,
      database: process.env.DB_NAME_DISKON,
      connectionLimit: 10, // Adjust the limit as per your requirements
    });

    console.log("Connected to WebDiskon database!");

    const poolToSimpi = mysql.createPool({
      host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASS,
      database: process.env.DB_NAME,
      connectionLimit: 10, // Adjust the limit as per your requirements
    });

    console.log("Connected to Simpi database!");
    return { poolToWebDiskon, poolToSimpi };
  } catch (err) {
    console.error("Error connecting to databases:", err);
    throw err;
  }
}

export default setupConnections;
