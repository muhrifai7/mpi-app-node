import fs from "fs";
import readline from "readline";

import chokidar from "chokidar";
import moment from "moment";
import xlsx from "xlsx";

import { getPoolToWebDiskon, getPoolToSimpi } from "../../config/db.js";

const root_folder = process.env.SOURCE_FILE;
const upload_path = process.env.UPLOAD_PATH;
const processed_path = process.env.PROCCESSED_FILE;
const failed_path = process.env.FAILED_FILE;
const source_folder = `${root_folder}/${upload_path}`;
const success_folder = `${root_folder}/${processed_path}`;
const failed_folder = `${root_folder}/${failed_path}`;
const ROWS_PER_BATCH = 30;
const watcher = chokidar.watch(`${source_folder}`, {
  persistent: true,
  ignoreInitial: false,
});

const insertOrUpdateDataOutlet = async (
  data,
  table,
  poolToWebDiskon,
  poolToSimpi
) => {
  try {
    const outletSiteNumber = data.OUTLETSITENUMBER;
    // Check if the outlet already exists in the database based on the outletSiteNumber
    const checkExist = await poolToSimpi.query(
      `SELECT id FROM ${table} WHERE outletSiteNumber = ?`,
      [outletSiteNumber]
    );
    console.log(checkExist, "checkExist");
    if (checkExist && checkExist[0].length > 0) {
      // Outlet exists, so update the data in m_outlet
      const outletId = checkExist[0][0].id;
      const updateQuery = `UPDATE ${table} SET
        outletSiteNumber = ?,
        idbranch = ?,
        outletStatus = ?,
        outletKlas = ?,
        outletPelanggan = ?,
        outletAlamat = ?,
        outletCodeColl = ?,
        outletCity = ?,
        outletCustNumber = ?
            WHERE id = ?`;

      const updateData = [
        data.OUTLETSITENUMBER,
        data.BRANCHID,
        data.STATUS_OUTLET,
        data.OUTLETKLAS,
        data.OUTLETPELANGGAN,
        data.OUTLETALAMAT,
        data.OUTLETCODECOLL,
        data.OUTLETCITY,
        data.CUST_ID,
        outletId,
      ];
      await poolToSimpi.query(updateQuery, updateData);
      console.log(
        `Outlet with outletSiteNumber ${outletSiteNumber} updated successfully!`
      );
    } else {
      // Outlet does not exist, so insert the data into m_outlet
      const insertQuery = `INSERT INTO ${table} (
        outletSiteNumber,
        idbranch,
        outletStatus,
        outletKlas,
        outletPelanggan,
        outletAlamat,
        outletCodeColl,
        outletCity,
        outletCustNumber
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;

      const insertData = [
        data.OUTLETSITENUMBER,
        data.BRANCHID,
        data.STATUS_OUTLET,
        data.OUTLETKLAS,
        data.OUTLETPELANGGAN,
        data.OUTLETALAMAT,
        data.OUTLETCODECOLL,
        data.OUTLETCITY,
        data.CUST_ID,
      ];
      console.log(insertData, "insertData 1");
      await poolToSimpi.query(insertQuery, insertData);
      console.log(
        `Outlet with outletSiteNumber ${outletSiteNumber} inserted successfully!`
      );
    }

    const checkExistDbDiskon = await poolToWebDiskon.query(
      `SELECT outlet_id FROM ${table} WHERE outletSiteNumber = ?`,
      [outletSiteNumber]
    );
    if (checkExistDbDiskon && checkExistDbDiskon[0].length > 0) {
      // Outlet exists, so update the data in m_outlet
      const outlet_id = checkExistDbDiskon[0][0].outlet_id;
      const updateData = [
        data.OUTLETSITENUMBER,
        data.BRANCHID,
        data.STATUS_OUTLET,
        data.OUTLETKLAS,
        data.OUTLETPELANGGAN,
        data.OUTLETALAMAT,
        data.OUTLETCODECOLL,
        data.OUTLETCITY,
        data.CUST_ID,
        data.CUSTOMERNUMBER,
        data.PARTYSITEID,
        data.REFERENCESITENUMBER,
        outlet_id,
      ];
      const updateQueryDiskon = `UPDATE ${table} SET
        outletSiteNumber = ?,
        branchId = ?,
        outletStatus = ?,
        outletKlas = ?,
        outletPelanggan = ?,
        outletAlamat = ?,
        outletCodeColl = ?,
        outletCity = ?,
        cust_id = ?,
        customerNumber = ?,
        partySiteId = ?,
        siteNumberRefrences = ?
            WHERE outlet_id = ?`;

      let result = await poolToWebDiskon.query(updateQueryDiskon, updateData);
      console.log(
        `Outlet with outletSiteNumber ${outletSiteNumber} updated successfully!`
      );
    } else {
      console.log("elssse");
      const insertData = [
        data.OUTLETSITENUMBER,
        data.BRANCHID,
        data.STATUS_OUTLET,
        data.OUTLETKLAS,
        data.OUTLETPELANGGAN,
        data.CUSTOMERNUMBER,
        data.OUTLETALAMAT,
        data.PARTYSITEID,
      ];
      const insertQueryDiskon = `INSERT INTO ${table} (
        outletSiteNumber,
        branchId,
        outletStatus,
        outletKlas,
        outletPelanggan,
        customerNumber,
        outletAlamat,
        partySiteId
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;
      await poolToWebDiskon.query(insertQueryDiskon, insertData);
      console.log(
        `Outlet with outletSiteNumber ${outletSiteNumber} inserted successfully!`
      );
    }
  } catch (err) {
    console.log(err, "err");
    throw err;
  }
};

const insertOrUpdateDataItem = async (
  data,
  table,
  poolToWebDiskon,
  poolToSimpi
) => {
  try {
    const itemInventoryItemId = data.INVENTORY_ITEM_ID;
    // Check if the outlet already exists in the database based on the outletSiteNumber
    const checkExist = await poolToSimpi.query(
      `SELECT itemInventoryItemId FROM ${table} WHERE itemInventoryItemId = ?`,
      [itemInventoryItemId]
    );
    if (checkExist && checkExist[0].length > 0) {
      // Outlet exists, so update the data in m_outlet
      const updateQuery = `UPDATE ${table} SET
      itemInventoryItemId = ?,
      itemSupId = ?,
      itemProduk = ?,
      itemUom = ?,
      itemSatuanKecil = ?,
      itemClassProduk = ?,
      itemIDprinc = ?,
      itemHna = ?,
      itemClassName = ?
            WHERE itemInventoryItemId = ?`;

      const updateData = [
        data.INVENTORY_ITEM_ID,
        data.SUPLIER_ID,
        data.PRODUK,
        data.UOM,
        data.SATUAN_KECIL,
        data.CLASS_PROD,
        data.PRINCIPAL,
        data.HNA,
        data.CLASS_NAME,
        itemInventoryItemId,
      ];

      await poolToSimpi.query(updateQuery, updateData);
      console.log(
        `Outlet with INVENTORY_ITEM_ID ${itemInventoryItemId} updated successfully!`
      );
    } else {
      // Outlet does not exist, so insert the data into m_outlet
      const insertQuery = `INSERT INTO ${table} (
        itemInventoryItemId,
        itemCode,
        itemSupId,
        itemProduk,
        itemUom,
        itemSatuanKecil,
        itemClassProduk,
        itemIDprinc,
        itemClassName,
        itemHna
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

      const insertData = [
        data.INVENTORY_ITEM_ID,
        data.ITEM,
        data.SUPLIER_ID,
        data.PRODUK,
        data.UOM,
        data.SATUAN_KECIL,
        data.CLASS_PROD,
        data.PRINCIPAL,
        data.CLASS_NAME,
        data.HNA,
      ];

      await poolToSimpi.query(insertQuery, insertData);
      console.log(
        `Outlet with INVENTORY_ITEM_ID ${itemInventoryItemId} inserted successfully!`
      );
    }
    const checkExistDbDiskon = await poolToWebDiskon.query(
      `SELECT itemInventoryItemId FROM ${table} WHERE itemInventoryItemId = ?`,
      [itemInventoryItemId]
    );
    if (checkExistDbDiskon && checkExistDbDiskon[0].length > 0) {
      // Outlet exists, so update the data in m_outlet
      const updateQuery = `UPDATE ${table} SET
      itemInventoryItemId = ?,
      itemSupId = ?,
      itemProduk = ?,
      itemUom = ?,
      itemSatuanKecil = ?,
      itemClassProduk = ?,
      itemIDprinc = ?,
      itemClassName = ?
            WHERE itemInventoryItemId = ?`;

      const updateData = [
        data.INVENTORY_ITEM_ID,
        data.SUPLIER_ID,
        data.PRODUK,
        data.UOM,
        data.SATUAN_KECIL,
        data.CLASS_PROD,
        data.PRINCIPAL,
        data.CLASS_NAME,
        itemInventoryItemId,
      ];

      await poolToWebDiskon.query(updateQuery, updateData);
      console.log(
        `Outlet with INVENTORY_ITEM_ID ${itemInventoryItemId} updated successfully!`
      );
    } else {
      // Outlet does not exist, so insert the data into m_outlet
      const insertQuery = `INSERT INTO ${table} (
        itemInventoryItemId,
        itemCode,
        itemSupId,
        itemProduk,
        itemUom,
        itemSatuanKecil,
        itemClassProduk,
        itemIDprinc,
        itemClassName
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;

      const insertData = [
        data.INVENTORY_ITEM_ID,
        data.ITEM,
        data.SUPLIER_ID,
        data.PRODUK,
        data.UOM,
        data.SATUAN_KECIL,
        data.CLASS_PROD,
        data.PRINCIPAL,
        data.CLASS_NAME,
      ];

      await poolToWebDiskon.query(insertQuery, insertData);
      console.log(
        `Outlet with INVENTORY_ITEM_ID ${itemInventoryItemId} inserted successfully!`
      );
    }
  } catch (err) {
    throw err;
  }
};

const insertOrUpdateDataDofoProblemServer = async (
  dataChunks,
  table,
  poolToSimpi
) => {
  try {
    console.log(dataChunks.length, "length");
    const insertQuery = `INSERT INTO ${table} (
      NO_PERFORMA,
      TGL_INVOICE,
      PERIODE,
      NO_INVOICE,
      SITE_NUMBER,
      PARTY_NAME,
      DAYS,
      QTY,
      KODELANG,
      SALES,
      SALES_TYPE
    ) VALUES ?`;

    // Start a transaction to improve performance
    await poolToSimpi.query("START TRANSACTION");
    const insertData = dataChunks.map((data) => [
      data.TGL_INVOICE,
      data.YEAR,
      data.BLN,
      data.DAYS,
      data.PERIODE,
      data.JENIS,
      data.NO_PERFORMA,
      data.NO_INVOICE,
      data.SITE_NUMBER,
      data.SALES,
      data.QTY,
      data.OFF_PRINC,
      data.OFF_MPI,
      data.CNTARIK,
      data.ON_MPI,
      data.ON_PRIN,
      data.BONUS,
      data.GRUPLANG,
      data.KODELANG,
      data.SALES_TYPE,
      data.NAMALANG,
      data.ALMTLANG,
      data.CITY,
      data.KLSOUT,
      data.PARTY_NAME,
      data.NO_DPL,
      data.PRICE_ID,
      "",
      "",
    ]);
    let execQuery = await poolToSimpi.query(insertQuery, [insertData]);
    console.log(execQuery, "execQuery");

    await poolToSimpi.query("COMMIT");

    console.log(`Data inserted into table ${table} successfully!`);
  } catch (err) {
    await poolToSimpi.query("ROLLBACK");
    console.log(err, "err");
    throw err;
  }
};

const insertOrUpdateDataDofoTEST = async (data, table, poolToSimpi) => {
  try {
    const batchSize = 1000;
    const insertQuery = `INSERT INTO ${table} (
      NO_PERFORMA,
      TGL_INVOICE,
      PERIODE,
      NO_INVOICE,
      SITE_NUMBER,
      PARTY_NAME,
      DAYS,
      QTY,
      KODELANG,
      SALES,
      SALES_TYPE
    ) VALUES ?`;

    const dataChunks = [];
    for (let i = 0; i < data.length; i += batchSize) {
      dataChunks.push(data.slice(i, i + batchSize));
    }
    // Start a transaction to improve performance
    await poolToSimpi.query("START TRANSACTION");
    for (const chunk of dataChunks) {
      const insertData = chunk.map((row) => [
        row.NO_PERFORMA,
        row.TGL_INVOICE,
        row.PERIODE,
        row.NO_INVOICE,
        row.PARTY_NUMBER,
        row.PARTY_NAME,
        row.DAYS,
        row.QTY,
        row.KODE_PRODUCT,
        // PRODUK,
        // UOM,
        // HNA,
        row.SALES,
        // IDSUP,
        // NO_FDK,
        row.SALES_TYPE,
        // TYPE_ORDER,
        // ONGKIR,
        // ATTR1,
      ]);
      let execQuery = await poolToSimpi.query(insertQuery, [insertData]);
      console.log(execQuery, "execQuery'");
    }
    await poolToSimpi.query("COMMIT");

    console.log(`Data inserted into table ${table} successfully!`);
  } catch (err) {
    console.log(err, "err");
    // throw new err();
  }
};

const insertOrUpdateDataDofoNew = async (
  data,
  table,
  poolToWebDiskon,
  poolToSimpi
) => {
  try {
    // Outlet does not exist, so insert the data into m_outlet
    const insertQuery = `INSERT INTO ${table} (
      NO_PERFORMA,
      TGL_INVOICE,
      PERIODE,
      NO_INVOICE,
      SITE_NUMBER,
      PARTY_NAME,
      DAYS,
      QTY,
      KODELANG,
      SALES,
      SALES_TYPE
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

    const insertData = [
      data.NO_PERFORMA,
      data.TGL_INVOICE,
      data.PERIODE,
      data.NO_INVOICE,
      data.PARTY_NUMBER,
      data.PARTY_NAME,
      data.DAYS,
      data.QTY,
      data.KODE_PRODUCT,
      // PRODUK,
      // UOM,
      // HNA,
      data.SALES,
      // IDSUP,
      // NO_FDK,
      data.SALES_TYPE,
      // TYPE_ORDER,
      // ONGKIR,
      // ATTR1,
    ];

    await poolToSimpi.query(insertQuery, insertData);
    console.log(
      `trx_sales  with no_performa ${data.NO_PERFORMA} inserted successfully!`
    );
  } catch (err) {
    throw err;
  }
};

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
        console.log(csvData, "csvData");
        for (const data of csvData) {
          await insertOrUpdateDataOutlet(
            data,
            table,
            poolToWebDiskon,
            poolToSimpi
          );
        }
        const newFileName = `${success_folder}/${fileName}`;
        console.log(newFileName, "newFileName");
        if (fs.existsSync(path)) {
          fs.rename(path, newFileName, (err) => {
            if (err) {
              console.log(`Error while renaming after insert: ${err.message}`);
            } else {
              console.log(
                `Succeed to process and moved file to: ${newFileName}`
              );
            }
          });
        } else {
          console.log(`Error: The file ${path} does not exist.`);
        }
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
  if (fileName.toUpperCase().indexOf("M_ITEM") != -1) {
    setTimeout(async () => {
      try {
        const workbook = xlsx.readFile(path, { raw: true });
        const sheet = workbook.SheetNames[0];
        const csvData = xlsx.utils.sheet_to_json(workbook.Sheets[sheet]);
        const table = "m_item";
        for (const data of csvData) {
          await insertOrUpdateDataItem(
            data,
            table,
            poolToWebDiskon,
            poolToSimpi
          );
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
  if (fileName.toUpperCase().indexOf("DOFO_SALES_GAGAL") != -1) {
    setTimeout(async () => {
      try {
        const workbook = xlsx.readFile(path, { raw: true });
        const sheet = workbook.SheetNames[0];
        const csvData = xlsx.utils.sheet_to_json(workbook.Sheets[sheet]);
        const table = "transaksi_sales";
        const truncateQuery = `TRUNCATE TABLE ${table}`;
        await poolToSimpi.query(truncateQuery);
        await insertOrUpdateDataDofo(csvData, table, poolToSimpi);
        const newFileName = `${success_folder}/${fileName}`;
        await new Promise((resolve, reject) => {
          fs.rename(path, newFileName, (err) => {
            if (err) {
              console.log(`Error while renaming after insert: ${err.message}`);
              reject(err);
            } else {
              console.log(
                `Succeed to process and moved file to: ${newFileName}`
              );
              resolve();
            }
          });
        });
      } catch (error) {
        console.log(error, "error'");
        const newFileName = `${failed_folder}/${fileName}`;
        try {
          await fs.promises.rename(path, newFileName);
          console.log(`Failed to process and moved file to: ${newFileName}`);
        } catch (err) {
          console.log(`Error while moving Failed file : ${err.message}`);
        }
      }
    }, 800);
  }
  // bulk insert
  if (fileName.toUpperCase().indexOf("DOFO_SALES_BlUM_BISA") != -1) {
    setTimeout(async () => {
      try {
        console.log("Reading dofo sales started");
        let rowCount = 0;
        let batchRows = [];

        const readStream = fs.createReadStream(path, {
          encoding: "utf8",
          highWaterMark: 256 * 1024, // Set the buffer size to 64 KB (adjust as needed)
        });
        console.log(readStream, "readStream");
        const rl = readline.createInterface({
          input: readStream,
          crlfDelay: Infinity, // To handle CRLF line endings on Windows
        });
        console.log(rl, "rl");

        const table = "transaksi_sales";
        const truncateQuery = `TRUNCATE TABLE ${table}`;
        await poolToSimpi.query(truncateQuery);
        rl.on("line", async (line) => {
          rowCount++;
          const data = parseCSVLine(line); // Implement this function to parse CSV lines
          batchRows.push(data);
          if (rowCount === ROWS_PER_BATCH) {
            await insertOrUpdateDataDofo(batchRows, table, poolToSimpi);
            batchRows = [];
            rowCount = 0;
          }
        });

        rl.on("close", async () => {
          if (batchRows.length > 0) {
            // await insertBulkData(batchRows, table);
            await insertOrUpdateDataDofo(batchRows, table, poolToSimpi);
          }
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
    }, 800);
  }
  if (fileName.toUpperCase().indexOf("DOFO_SALES") != -1) {
    setTimeout(async () => {
      try {
        const workbook = xlsx.readFile(path, { raw: true });
        const sheet = workbook.SheetNames[0];
        const csvData = xlsx.utils.sheet_to_json(workbook.Sheets[sheet]);
        const table = "transaksi_sales";
        const truncateQuery = `TRUNCATE TABLE ${table}`;
        await poolToSimpi.query(truncateQuery);
        for (const data of csvData) {
          await insertOrUpdateDataDofoNew(
            data,
            table,
            poolToWebDiskon,
            poolToSimpi
          );
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

const parseCSVLine = (line) => {
  const columns = line.split(";");
  const [
    CABANG,
    BRANCHID,
    HRSO,
    NO_INVOICE,
    NO_PERFORMA,
    JENIS,
    GRUPLANG,
    KODELANG,
    SITE_NUMBER,
    PARTY_NAME,
    NAMALANG,
    ALMTLANG,
    KLSOUT,
    DAYS,
    BLN,
    PERIODE,
    KODE_PRODUCT,
    PRODUK,
    UOM,
    KEMASAN,
    PRINCIPAL,
    QTY,
    HNA,
    SALES,
    CLASSPROD,
    KETKLASP,
    IDSUP,
    ON_MPI,
    ONMPI,
    ON_PRIN,
    ONPRINC,
    ASKES,
    BONUS,
    PERBONUS,
    CN_TARIK,
    OFFMPI,
    NO_FDK,
    OFFPRINC,
    NOMORF,
    TGLF,
    NO_DPL,
    OFF_MPI,
    OFF_PRINC,
    CNTARIK,
    CITY,
    TGL_INVOICE,
    BRANCHCODE,
    YEAR,
    SALES_TYPE,
    INTERFACE_LINE_ATTRIBUTE5,
    TYPE_ORDER,
    PO_NUMBER,
    ONGKIR,
    PRICE_ID,
    ATTR1,
    ATTR2,
  ] = columns;

  return {
    CABANG,
    BRANCHID,
    HRSO,
    NO_INVOICE,
    NO_PERFORMA,
    JENIS,
    GRUPLANG,
    KODELANG,
    SITE_NUMBER,
    PARTY_NAME,
    NAMALANG,
    ALMTLANG,
    KLSOUT,
    DAYS,
    BLN,
    PERIODE,
    KODE_PRODUCT,
    PRODUK,
    UOM,
    KEMASAN,
    PRINCIPAL,
    QTY,
    HNA,
    SALES,
    CLASSPROD,
    KETKLASP,
    IDSUP,
    ON_MPI,
    ONMPI,
    ON_PRIN,
    ONPRINC,
    ASKES,
    BONUS,
    PERBONUS,
    CN_TARIK,
    OFFMPI,
    NO_FDK,
    OFFPRINC,
    NOMORF,
    TGLF,
    NO_DPL,
    OFF_MPI,
    OFF_PRINC,
    CNTARIK,
    CITY,
    TGL_INVOICE,
    BRANCHCODE,
    YEAR,
    SALES_TYPE,
    INTERFACE_LINE_ATTRIBUTE5,
    TYPE_ORDER,
    PO_NUMBER,
    ONGKIR,
    PRICE_ID,
    ATTR1,
    ATTR2,
  };
};

export default watcher;
