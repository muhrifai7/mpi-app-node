// const directory = "/src/fonts";
import fs from "fs";
import path from "path";
import schedule from "node-schedule";
const root_folder = process.env.SOURCE_FILE;
const processedpath = process.env.PROCCESSED_FILE;
const success_folder = `${root_folder}/${processedpath}`;

// function ini jalan setiap jam 1 pagi, untuk mengapus file dengan masa 1 hari
const removeOldFile = schedule.scheduleJob("0 0 * * *", async () => {
  const second = 60 * 60 * 30;
  const currentTime = Math.floor(new Date().getTime() / 1000);
  const files = Array.from(await fs.readdir(success_folder)).filter((file) => {
    return ext.indexOf(file.split(".").at(-1)) !== -1;
  });
  for (const file of files) {
    const filePath = path.join(success_folder, file);
    const stats = await fs.stat(filePath); // age of file
    const birthTime = Math.floor(stats.ctimeMs / 1000);
    if (currentTime - birthTime > second) {
      console.log(`Remove ${filePath} file success!`);
      fs.rm(filePath);
    }
  }
  console.log(`This runs at 00:00AM every day, Clean data on ${processedPath}`);
});
