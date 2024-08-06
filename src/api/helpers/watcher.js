import fs from "fs";
import readline from "readline";
import chokidar from "chokidar";
import xlsx from "xlsx";

import { getPoolToWebDiskon, getPoolToSimpi } from "../../config/db.js";
import { deleteCycleCount } from "../fusion/cycleCount.js";
import { upsertDataOutlet } from "../controllers/outlet/upsertDataOutlet.js";
import { upsertDataItem } from "../controllers/item/upsertDataItem.js";
import { batchInsertDataDofo } from "../controllers/dofo/transaksi.js";
import { parseCSVLine } from "./parseCsv.js";
import { parseCSVLineNew } from "./parseCsvNew.js";

const root_folder = process.env.SOURCE_FILE;
const upload_path = process.env.UPLOAD_PATH;
const processed_path = process.env.PROCCESSED_FILE;
const failed_path = process.env.FAILED_FILE;
const source_folder = `${root_folder}/${upload_path}`;
const success_folder = `${root_folder}/${processed_path}`;
const failed_folder = `${root_folder}/${failed_path}`;
const watcher = chokidar.watch(`${source_folder}`, {
  persistent: true,
  ignoreInitial: false,
});

watcher.on("ready", () => {
  console.log(`Watcher is ready and scanning files on ${source_folder}`);
});
const poolToWebDiskon = await getPoolToWebDiskon();
const poolToSimpi = await getPoolToSimpi();

watcher.on("add", async (path) => {
  const fileName = path.split("/").slice(-1)[0];
  if (fileName.toUpperCase().indexOf("M_OUTLET") != -1) {
    setTimeout(async () => {
      try {
        const workbook = xlsx.readFile(path, { raw: true });
        const sheet = workbook.SheetNames[0];
        const csvData = xlsx.utils.sheet_to_json(workbook.Sheets[sheet]);
        const table = "m_outlet";
        for (const data of csvData) {
          await upsertDataOutlet(data, table, poolToWebDiskon, poolToSimpi);
        }
        const newFileName = `${success_folder}/${fileName}`;
        fs.rename(path, newFileName, (err) => {
          if (err) {
            console.log(`Error while renaming after insert: ${err.message}`);
          } else {
            console.log(`Succeed to process and moved file to: ${newFileName}`);
          }
        });
      } catch (error) {
        console.log(error, "error");
        const newFileName = `${failed_folder}/${fileName}`;
        console.log(newFileName, "newFileName");
        fs.renameSync(path, newFileName, (err) => {
          if (err) {
            console.log(`Error while moving Failed file : ${err.message}`);
          } else {
            console.log(`Failed to process and moved file to: ${newFileName}`);
          }
        });
      }
    }, 800);
  }
  if (fileName.toUpperCase().indexOf("ITEM_WEB_NEW") != -1) {
    setTimeout(async () => {
      try {
        const workbook = xlsx.readFile(path, { raw: true });
        const sheet = workbook.SheetNames[0];
        const csvData = xlsx.utils.sheet_to_json(workbook.Sheets[sheet]);
        const table = "m_item";
        for (const data of csvData) {
          await upsertDataItem(data, table, poolToWebDiskon, poolToSimpi);
        }
        const newFileName = `${success_folder}/${fileName}`;
        fs.rename(path, newFileName, (err) => {
          if (err) {
            console.log(`Error while renaming after insert: ${err.message}`);
          } else {
            console.log(`Succeed to process and moved file to: ${newFileName}`);
          }
        });
      } catch (error) {
        const newFileName = `${failed_folder}/${fileName}`;
        fs.renameSync(path, newFileName, (err) => {
          if (err) {
            console.log(`Error while moving Failed file : ${err.message}`);
          } else {
            console.log(`Failed to process and moved file to: ${newFileName}`);
          }
        });
      }
    }, 800);
  }
  // bulk insert
  if (fileName.toUpperCase().indexOf("DOFO_SALES_GAGAL") != -1) {
    setTimeout(async () => {
      try {
        const readStream = fs.createReadStream(path, {
          encoding: "utf8",
          highWaterMark: 256 * 1024, // Set the buffer size to 64 KB (adjust as needed)
        });
        const rl = readline.createInterface({
          input: readStream,
          crlfDelay: Infinity, // To handle CRLF line endings on Windows
        });

        const table = "transaksi_sales";
        const truncateQuery = `TRUNCATE TABLE ${table}`;
        await poolToSimpi.query(truncateQuery);
        let isFirstLine = true;
        rl.on("line", async (line) => {
          const data = parseCSVLineNew(line, isFirstLine);
          isFirstLine = false;
          await batchInsertDataDofo(data, table, poolToSimpi);
        });

        rl.on("close", async () => {
          const newFileName = `${success_folder}/${fileName}`;
          fs.rename(path, newFileName, (err) => {
            if (err) {
              console.log(`Error while renaming after insert: ${err.message}`);
            } else {
              console.log(
                `Succeed to process and moved file to: ${newFileName}`
              );
            }
          });
          console.log("close");
        });

        rl.on("error", (err) => {
          console.error("Error while reading the file:", err);
          const newFileName = `${failed_folder}/${fileName}`;
          fs.renameSync(path, newFileName, (err) => {
            if (err) {
              console.log(`Error while moving Failed file : ${err.message}`);
            } else {
              console.log(
                `Failed to process and moved file to: ${newFileName}`
              );
            }
          });
        });
      } catch (error) {
        console.log(error, "error'");
        const newFileName = `${failed_folder}/${fileName}`;
        fs.renameSync(path, newFileName, (err) => {
          if (err) {
            console.log(`Error while moving Failed file : ${err.message}`);
          } else {
            console.log(`Failed to process and moved file to: ${newFileName}`);
          }
        });
      }
    }, 1000);
  }
  if (fileName.toUpperCase().indexOf("DOFO_SALES") != -1) {
    setTimeout(async () => {
      try {
        console.log("Reading dofo sales started new");
        const workbook = xlsx.readFile(path, { raw: true });
        const sheet = workbook.SheetNames[0];
        const csvData = xlsx.utils.sheet_to_json(workbook.Sheets[sheet]);
        const table = "transaksi_sales";
        const truncateQuery = `TRUNCATE TABLE ${table}`;
        await poolToSimpi.query(truncateQuery);
        for (const data of csvData) {
          await batchInsertDataDofo(data, table, poolToSimpi);
        }
        const newFileName = `${success_folder}/${fileName}`;
        fs.rename(path, newFileName, (err) => {
          if (err) {
            console.log(`Error while renaming after insert: ${err.message}`);
          } else {
            console.log(`Succeed to process and moved file to: ${newFileName}`);
          }
        });
      } catch (error) {
        const newFileName = `${failed_folder}/${fileName}`;
        fs.renameSync(path, newFileName, (err) => {
          if (err) {
            console.log(`Error while moving Failed file : ${err.message}`);
          } else {
            console.log(`Failed to process and moved file to: ${newFileName}`);
          }
        });
      }
    }, 800);
  }
  if (fileName.toUpperCase().indexOf("CYCLE_COUNT") != -1) {
    setTimeout(async () => {
      try {
        const workbook = xlsx.readFile(path, { raw: true });
        const sheet = workbook.SheetNames[0];
        const csvData = xlsx.utils.sheet_to_json(workbook.Sheets[sheet]);
        // const table = "m_outlet";
        for (const data of csvData) {
          if (data.RESERVATION_ID) {
            await deleteCycleCount(data.RESERVATION_ID);
          }
        }
        const newFileName = `${success_folder}/${fileName}`;
        fs.rename(path, newFileName, (err) => {
          if (err) {
            console.log(`Error while renaming after insert: ${err.message}`);
          } else {
            console.log(`Succeed to process and moved file to: ${newFileName}`);
          }
        });
      } catch (error) {
        console.log(error, "error");
        const newFileName = `${failed_folder}/${fileName}`;
        console.log(newFileName, "newFileName");
        fs.renameSync(path, newFileName, (err) => {
          if (err) {
            console.log(`Error while moving Failed file : ${err.message}`);
          } else {
            console.log(`Failed to process and moved file to: ${newFileName}`);
          }
        });
      }
    }, 800);
  }
});

export default watcher;
