import moment from "moment";

export const upsertDataItem = async (
  data,
  table,
  poolToWebDiskon,
  poolToSimpi
) => {
  try {
    const itemInventoryItemId = data.ITEMINVENTORYITEMID;
    // Check if the outlet already exists in the database based on the outletSiteNumber
    const checkExist = await poolToSimpi.query(
      `SELECT itemInventoryItemId FROM ${table} WHERE itemInventoryItemId = ?`,
      [itemInventoryItemId]
    );
    if (checkExist && checkExist[0].length > 0) {
      // Outlet exists, so update the data in m_outlet
      const updateQuery = `UPDATE ${table} SET
        itemCode = ?,
        itemSupId = ?,
        itemProduk = ?,
        itemUom = ?,
        itemClassProduk = ?,
        itemSatuanKecil = ?,
        itemClassName = ?,
        itemIDprinc = ?,
        itemHna = ?,
        itemLogoObat = ?,
        itemKdOtc = ?,
        itemType = ?,
        type = ?,
        type_code = ?
              WHERE itemInventoryItemId = ?`;

      const updateData = [
        data.ITEMCODE,
        data.ITEMSUPID,
        data.ITEMPRODUK,
        data.ITEMUOM,
        data.ITEMCLASSPROD,
        data.ITEMSATUANKECIL,
        data.ITEMCLASSNAME,
        data.ITEMIDPRINC,
        data.ITEMHNA,
        data.ITEMLOGOOBAT,
        data.ITEMKDOTC,
        data.ITEMTYPE,
        data.TYPE,
        data.TYPE_CODE,
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
          itemClassProduk,
          itemSatuanKecil,
          itemClassName,
          itemIDprinc,
          itemHna,
          itemLogoObat,
          itemKdOtc,
          itemType,
          type,
          type_code
            ) VALUES (?, ?, ?, ?, ?,
                      ?, ?, ?, ?, ?,
                      ?, ?, ?, ?, ?)`;

      const insertData = [
        data.ITEMINVENTORYITEMID,
        data.ITEMCODE,
        data.ITEMSUPID,
        data.ITEMPRODUK,
        data.ITEMUOM,
        data.ITEMCLASSPROD,
        data.ITEMSATUANKECIL,
        data.ITEMCLASSNAME,
        data.ITEMIDPRINC,
        data.ITEMHNA,
        data.ITEMLOGOOBAT,
        data.ITEMKDOTC,
        data.ITEMTYPE,
        data.TYPE,
        data.TYPE_CODE,
      ];
      console.log(insertData, "insertData");

      await poolToSimpi.query(insertQuery, insertData);
      console.log(
        `Outlet with INVENTORY_ITEM_ID ${itemInventoryItemId} inserted successfully!`
      );
    }
  } catch (err) {
    console.log(err, "err");
    throw err;
  }
};
