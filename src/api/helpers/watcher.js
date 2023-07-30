import fs from "fs";
import fsp from "fs/promises";
import chokidar from "chokidar";
import moment from "moment";
import xlsx from "xlsx";

import setupConnections from "../../config/db.js";

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

const insertOrUpdateDataOutlet = async (data, table) => {
  try {
    const { connectionToWebDiskon, connectionToSimpi } =
      await setupConnections();

    const outletSiteNumber = data.OUTLETSITENUMBER;
    // Check if the outlet already exists in the database based on the outletSiteNumber
    const checkExist = await connectionToSimpi.query(
      `SELECT id FROM ${table} WHERE outletSiteNumber = ?`,
      [outletSiteNumber]
    );
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
      await connectionToSimpi.query(updateQuery, updateData);
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
      await connectionToSimpi.query(insertQuery, insertData);
      console.log(
        `Outlet with outletSiteNumber ${outletSiteNumber} inserted successfully!`
      );
    }

    const checkExistDbDiskon = await connectionToWebDiskon.query(
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

      console.log(updateData, "updateData");
      let result = await connectionToWebDiskon.query(
        updateQueryDiskon,
        updateData
      );
      console.log(result, "result");
      console.log(
        `Outlet with outletSiteNumber ${outletSiteNumber} updated successfully!`
      );
    } else {
      const insertData = [
        data.OUTLETSITENUMBER,
        data.BRANCHID,
        data.STATUS_OUTLET,
        data.OUTLETKLAS,
        data.OUTLETPELANGGAN,
        // data.OUTLETKRLIMIT,
        data.OUTLETALAMAT,
        data.OUTLETCODECOLL,
        data.OUTLETCITY,
        data.CUSTOMERID,
      ];
      const insertQueryDiskon = `INSERT INTO ${table} (
        outletSiteNumber,
        branchId,
        outletStatus,
        outletKlas,
        outletPelanggan,
        outletAlamat,
        outletCodeColl,
        outletCity,
        cust_id,
        customerNumber,
        partySiteId,
        siteNumberRefrences
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

      await connectionToWebDiskon.query(insertQueryDiskon, insertData);
      console.log(
        `Outlet with outletSiteNumber ${outletSiteNumber} inserted successfully!`
      );
    }
  } catch (err) {
    throw new err();
  }
};

const insertOrUpdateDataItem = async (data, table) => {
  try {
    const { connectionToWebDiskon, connectionToSimpi } =
      await setupConnections();
    const itemInventoryItemId = data.INVENTORY_ITEM_ID;
    // Check if the outlet already exists in the database based on the outletSiteNumber
    const checkExist = await connectionToSimpi.query(
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

      await connectionToSimpi.query(updateQuery, updateData);
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

      await connectionToSimpi.query(insertQuery, insertData);
      console.log(
        `Outlet with INVENTORY_ITEM_ID ${itemInventoryItemId} inserted successfully!`
      );
    }
    const checkExistDbDiskon = await connectionToWebDiskon.query(
      `SELECT itemInventoryItemId FROM ${table} WHERE itemInventoryItemId = ?`,
      [itemInventoryItemId]
    );
    console.log(checkExistDbDiskon, "hayaaa");
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

      await connectionToWebDiskon.query(updateQuery, updateData);
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

      await connectionToWebDiskon.query(insertQuery, insertData);
      console.log(
        `Outlet with INVENTORY_ITEM_ID ${itemInventoryItemId} inserted successfully!`
      );
    }
  } catch (err) {
    throw new err();
  }
};

const insertOrUpdateDataDofo = async (data, table) => {
  try {
    const { connectionToWebDiskon, connectionToSimpi } =
      await setupConnections();
    console.log(data, "data");
    const itemInventoryItemId = data.INVENTORY_ITEM_ID;
    // Check if the outlet already exists in the database based on the outletSiteNumber
    const checkExist = await connectionToSimpi.query(
      `SELECT org_id FROM ${table} WHERE org_id = ?`,
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

      await connectionToSimpi.query(updateQuery, updateData);
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

      await connectionToSimpi.query(insertQuery, insertData);
      console.log(
        `Outlet with INVENTORY_ITEM_ID ${itemInventoryItemId} inserted successfully!`
      );
    }
  } catch (err) {
    throw new err();
  }
};

watcher.on("ready", () => {
  console.log(`Watcher is ready and scanning files on ${source_folder}`);
  // You can optionally process existing files here if needed
});

watcher.on("add", async (path) => {
  console.log(path, "path");
  const fileName = path.split("/").slice(-1)[0];
  if (fileName.toUpperCase().indexOf("M_OUTLET") != -1) {
    setTimeout(async () => {
      try {
        const workbook = xlsx.readFile(path, { raw: true });
        const sheet = workbook.SheetNames[0];
        const csvData = xlsx.utils.sheet_to_json(workbook.Sheets[sheet]);
        const table = "m_outlet";
        for (const data of csvData) {
          await insertOrUpdateDataOutlet(data, table);
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
  if (fileName.toUpperCase().indexOf("M_ITEM") != -1) {
    setTimeout(async () => {
      try {
        const workbook = xlsx.readFile(path, { raw: true });
        const sheet = workbook.SheetNames[0];
        const csvData = xlsx.utils.sheet_to_json(workbook.Sheets[sheet]);
        const table = "m_item";
        for (const data of csvData) {
          await insertOrUpdateDataItem(data, table);
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
  if (fileName.toUpperCase().indexOf("DOFO_SALES_NANTI_GANTI") != -1) {
    setTimeout(async () => {
      try {
        const workbook = xlsx.readFile(path, { raw: true });
        const sheet = workbook.SheetNames[0];
        const csvData = xlsx.utils.sheet_to_json(workbook.Sheets[sheet]);
        const table = "transaksi_sales";
        for (const data of csvData) {
          await insertOrUpdateDataDofo(data, table);
        }
        const newFileName = `${success_folder}/${fileName}`;
        // fs.rename(path, newFileName, (err) => {
        //   if (err) {
        //     console.log(`Error while renaming after insert: ${err.message}`);
        //   } else {
        //     console.log(`Succeed to process and moved file to: ${newFileName}`);
        //   }
        // });
      } catch (error) {
        const newFileName = `${failed_folder}/${fileName}`;
        // fs.renameSync(path, newFileName, (err) => {
        //   if (err) {
        //     console.log(`Error while moving Failed file : ${err.message}`);
        //   } else {
        //     console.log(`Failed to process and moved file to: ${newFileName}`);
        //   }
        // });
      }
    }, 800);
  }
});

export default watcher;
