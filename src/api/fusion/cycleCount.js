import { makeHttpRequest } from "../helpers/fetchApi.js";
import moment from "moment";

export async function deleteCycleCount(
  RESERVATION_ID
) {
  try {
    const response = await makeHttpRequest(`fscmRestApi/resources/11.13.18.05/inventoryReservations/${RESERVATION_ID}`, "DELETE", {});
    console.log(response, moment(), `run delete cycle count ${RESERVATION_ID}`);
  } catch (err) {
    console.log(err, "err");
    throw err;
  }
};