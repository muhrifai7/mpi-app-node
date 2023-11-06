// const apiUrl = `${process.env.REACT_APP_URL}`;
import fetch from 'node-fetch';
const apiUrl = `https://fa-evdm-saasfaprod1.fa.ocs.oraclecloud.com/`;
// const bearerToken = Cookies.get("token_bizops");
const base64 = Buffer.from(
    `mpisysadmin:mpi12345`
).toString('base64');

export async function makeHttpRequest(url, method, headers, body = null) {
  try {
    const response = await fetch(apiUrl + url, {
      method: method,
      headers: {
        ...headers,
        "Content-Type": "application/json",
        // Authorization: `Bearer ${bearerToken}`,
        Authorization: `Basic ${base64}`,
      },
      body: body ? JSON.stringify(body) : null,
      redirect: "follow",
    });

    //   console.log(response);

    if (response.status != '204') {
      throw new Error("Network response was not ok");
    }

    const data = {
        status: "true",
        message: "Delete Sukses"
    };
    return data;
  } catch (error) {
    console.error("An error occurred", error);
    throw error;
  }
}


