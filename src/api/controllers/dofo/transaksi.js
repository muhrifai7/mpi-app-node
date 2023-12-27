export const batchInsertDataDofo = async (data, table, poolToSimpi) => {
  try {
    const insertQuery = `INSERT INTO ${table} (
      ORG_ID,
      TGL_INVOICE,
      YEAR,
      BLN,
      DAYS,
      PERIODE,
      JENIS,
      NO_PERFORMA,
      NO_INVOICE,
      SITE_NUMBER,
      INVENTORY_ITEM_ID,
      SALES,
      PPN,
      QTY,
      UNIT_LIST_PRICE,
      UNIT_COST,
      UNIT_SELLING_PRICE,
      OFF_PRINC,
      OFF_MPI,
      CNTARIK,
      ON_MPI,
      ON_PRIN,
      BONUS,
      PHARGA,
      RPPHB,
      PERPHB,
      KET,
      KODESALUR,
      T_MULAI,
      T_AKHIR,
      XBULAN,
      XTAHUN,
      NOMORF,
      TGLF,
      RETURN_REASON,
      GRUPLANG,
      KODELANG,
      ID_PAKET,
      SALES_TYPE,
      NAMALANG,
      ALMTLANG,
      CITY,
      RAYON,
      KLSOUT,
      SALESREPID,
      PARTY_NAME,
      NO_DPL,
      ID_PRICE,
      key_po_item,
      key_po_outlet_item
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `;

    const insertData = [
      null,
      data?.TGL_INVOICE ? data?.TGL_INVOICE : null,
      data?.YEAR,
      data?.BLN,
      data?.DAYS,
      data?.PERIODE,
      data?.JENIS,
      isNaN(data?.NO_PERFORMA) ? null : data?.NO_PERFORMA,
      isNaN(data?.NO_INVOICE) ? null : data?.NO_INVOICE,
      data?.SITE_NUMBER,
      "", // INVENTORY_ITEM_ID
      data?.SALES,
      "", // PPN
      data?.QTY,
      "", // UNIT_LIST_PRICE
      "", // UNIT_COST
      "", // UNIT_SELLING_PRICE
      data?.OFF_PRINC,
      data?.OFF_MPI,
      data?.CNTARIK,
      data?.ON_MPI,
      data?.ON_PRIN,
      data?.BONUS,
      "", // PHARGA
      "", // RPPHB
      "", // PERPHB
      "", // KET
      null, // KODESALUR
      "", // T_MULAI,
      "", // T_AKHIR
      "", // XBULAN,
      "", // XTAHUN,
      data?.NOMORF,
      data?.TGLF ? data?.TGLF : null,
      "", //RETURN_REASON,
      data?.GRUPLANG,
      data?.KODELANG,
      "", // ID_PAKET
      data?.SALES_TYPE,
      data?.NAMALANG,
      data?.ALMTLANG,
      data?.CITY,
      "", // rayon
      data?.KLSOUT,
      "", // SALESREPID
      data?.PARTY_NAME,
      data?.NO_DPL,
      data?.PRICE_ID,
      "",
      "",
      // data?.CABANG,
      // data?.BRANCHID,
      // data?.HRSO,
      // data?.KODE_PRODUCT,
      // data?.PRODUK,
      // data?.UOM,
      // data?.KEMASAN,
      // data?.PRINCIPAL,
      // data?.HNA,
      // data?.CLASSPROD,
      // data?.KETKLASP,
      // data?.IDSUP,
      // data?.ONMPI,
      // data?.ONPRINC,
      // data?.ASKES,
      // data?.PERBONUS,
      // data?.NO_FDK,
      // data?.NOMORF,
      // data?.BRANCHCODE,
      // data?.INTERFACE_LINE_ATTRIBUTE5,
      // data?.TYPE_ORDER,
      // data?.PO_NUMBER,
      // data?.ONGKIR,
      // data?.ATTR1,
      // data?.ATTR2,
      // data?.CN_TARIK,
      // data?.OFFMPI,
      //   data?.OFFPRINC
    ];
    await poolToSimpi.query(insertQuery, insertData);
    console.log(
      `trx_sales  with no_performa ${data?.NO_PERFORMA} inserted successfully!`
    );
  } catch (err) {
    throw err;
  }
};
