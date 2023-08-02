// db.js

import dotenv from "dotenv";
dotenv.config();
import mysql from "mysql2/promise";

let poolToWebDiskon = null;
let poolToSimpi = null;

export async function getPoolToWebDiskon() {
  if (!poolToWebDiskon) {
    poolToWebDiskon = mysql.createPool({
      host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASS,
      database: process.env.DB_NAME_DISKON,
      connectionLimit: 10, // Adjust the limit as per your requirements
    });
    console.log("Connected to WebDiskon database!");
  }
  return poolToWebDiskon;
}

export async function getPoolToSimpi() {
  if (!poolToSimpi) {
    poolToSimpi = mysql.createPool({
      host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASS,
      database: process.env.DB_NAME,
      connectionLimit: 10, // Adjust the limit as per your requirements
    });
    console.log("Connected to Simpi database!");
  }
  return poolToSimpi;
}
