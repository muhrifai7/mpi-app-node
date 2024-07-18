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
      `SELECT outlet_id FROM ${table} WHERE outletSiteNumber = ?`,
      [outletSiteNumber]
    );
    if (checkExist && checkExist[0].length > 0) {
      // Outlet exists, so update the data in m_outlet
      const outletId = checkExist[0][0].outlet_id;
      const updateQuery = `UPDATE ${table} SET
          outletSiteNumber = ?,
          branchId = ?,
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
              WHERE outlet_id = ?`;

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
          branchId,
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
  } catch (err) {
    console.log(err, "err");
    throw err;
  }
};
