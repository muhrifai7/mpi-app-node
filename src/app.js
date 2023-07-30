import express from "express";
import routes from "./api/routes/index.js";
import dotenv from "dotenv";
dotenv.config();
const app = express();
const port = process.env.PORT || 3002;
import "./api/helpers/scheduler.js";

// Middleware to parse incoming JSON data
app.use(
  express.urlencoded({
    extended: true,
  })
);
app.use(express.json());

app.use("/api", routes);
// Start the server
app.listen(port, () => {
  console.log(`Server is running on ${process.env.ENV}:${port}`);
});
