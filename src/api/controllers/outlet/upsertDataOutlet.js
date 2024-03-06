import moment from "moment";

export const upsertDataOutlet = async (
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
          tax_code = ?,
          outletCustNumber = ?,
          sia = ?,
          sipa = ?,
          edout = ?,
          edsipa = ?,
          GroupOutlet = ?,
          telepone = ?,
          email = ?,
          npwp = ?,
          nama_npwp = ?,
          alamat_npwp = ?
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
        data.TAX_CODE,
        data.CUST_ID,
        data.SIA,
        data.SIPA,
        data.EDOUT ? moment(data.EDOUT, "DD/MM/YYYY").format("YYYY-MM-DD") : "",
        data.EDSIPA
          ? moment(data.EDSIPA, "DD/MM/YYYY").format("YYYY-MM-DD")
          : "",
        data.GROUP_OUTLET,
        data.TELEPON,
        data.EMAIL,
        data?.NPWP,
        data?.NAMA_NPWP,
        data?.ALAMAT_NPWP,
        outletId,
      ];
      await poolToSimpi.query(updateQuery, updateData);
      console.log(
        `Simpi Outlet with outletSiteNumber ${outletSiteNumber} updated successfully!`
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
          tax_code,
          outletCustNumber,
          sia,
          sipa,
          edout,
          edsipa,
          GroupOutlet,
          telepone,
          email,
          npwp,
          nama_npwp,
          alamat_npwp
            ) VALUES (?, ?, ?, ?, ?,
                      ?, ?, ?, ?, ?,
                      ?, ?, ?, ?, ?,
                      ?, ?, ?, ?, ?
                      )`;

      const insertData = [
        data.OUTLETSITENUMBER,
        data.BRANCHID,
        data.STATUS_OUTLET,
        data.OUTLETKLAS,
        data.OUTLETPELANGGAN,
        data.OUTLETALAMAT,
        data.OUTLETCODECOLL,
        data.OUTLETCITY,
        data.TAX_CODE,
        data.CUST_ID,
        data.SIA,
        data.SIPA,
        data.EDOUT ? moment(data.EDOUT, "DD/MM/YYYY").format("YYYY-MM-DD") : "",
        data.EDSIPA
          ? moment(data.EDSIPA, "DD/MM/YYYY").format("YYYY-MM-DD")
          : "",
        data.GROUP_OUTLET,
        data.TELEPON,
        data.EMAIL,
        data?.NPWP,
        data?.NAMA_NPWP,
        data?.ALAMAT_NPWP,
      ];
      console.log(insertData, "insertData 1", data);
      await poolToSimpi.query(insertQuery, insertData);
      console.log(
        `Simpi Outlet with outletSiteNumber ${outletSiteNumber} inserted successfully!`
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
        data.SITE_MEPRO,
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
          siteNumberRefrences = ?,
          site_mepro = ?
              WHERE outlet_id = ?`;

      await poolToWebDiskon.query(updateQueryDiskon, updateData);
      console.log(
        `Diskon Outlet with outletSiteNumber ${outletSiteNumber} updated successfully!`
      );
    } else {
      const insertData = [
        data.OUTLETSITENUMBER,
        data.BRANCHID,
        data.STATUS_OUTLET,
        data.OUTLETKLAS,
        data.OUTLETPELANGGAN,
        data.CUSTOMERNUMBER,
        data.OUTLETALAMAT,
        data.PARTYSITEID,
        data.SITE_MEPRO,
      ];
      const insertQueryDiskon = `INSERT INTO ${table} (
          outletSiteNumber,
          branchId,
          outletStatus,
          outletKlas,
          outletPelanggan,
          customerNumber,
          outletAlamat,
          partySiteId,
          site_mepro
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;
      await poolToWebDiskon.query(insertQueryDiskon, insertData);
      console.log(
        `Diskon Outlet with outletSiteNumber ${outletSiteNumber} inserted successfully!`
      );
    }
  } catch (err) {
    console.log(err, "err");
    throw err;
  }
};
