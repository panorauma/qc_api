import fs from "fs";
import path from "path";
import axios from "axios";
import dotenv from "dotenv";
import csv from "csv-parser"; // lightweight CSV reader

dotenv.config(); //need .env file inside tsver folder

type AnyDict = Record<string, any>;

const DEPLOY_ENVIRONMENT = ["dev", "prod"].includes(
  process.env.DEPLOY_ENVIRONMENT || "",
)
  ? process.env.DEPLOY_ENVIRONMENT!
  : "dev";
console.log(DEPLOY_ENVIRONMENT);

const API_BASE =
  DEPLOY_ENVIRONMENT === "prod"
    ? process.env.API_BASE || ""
    : "http://localhost:8000";
console.log(API_BASE);

const ASYNC_URL = `${API_BASE}/v1/validate`;
const ASYNC_STATUS_URL = `${API_BASE}/v1/validate/{task_id}`;
const SYNC_URL = `${API_BASE}/v1/validate/core`;

//handle files
async function fileToRows(filePath: string): Promise<AnyDict[]> {
  const ext = path.extname(filePath).toLowerCase();

  if (ext === ".csv") {
    return new Promise((resolve, reject) => {
      const rows: AnyDict[] = [];
      fs.createReadStream(filePath)
        .pipe(csv())
        .on("data", (row) => rows.push(row))
        .on("end", () => resolve(rows))
        .on("error", reject);
    });
  }

  if (ext === ".json") {
    const raw = fs.readFileSync(filePath, "utf8");
    const data = JSON.parse(raw);

    if (Array.isArray(data)) {
      if (data.length && typeof data[0] === "object") return data;
      throw new Error("JSON array must contain objects");
    } else if (data.rows) {
      return data.rows;
    } else if (typeof data === "object") {
      return [data];
    }
    throw new Error("Unsupported JSON format");
  }

  throw new Error(`Unsupported file extension: ${ext}. Use .csv or .json`);
}

async function buildRequestBody(
  datasetFile: string,
  datadicFile: string,
): Promise<AnyDict> {
  const datasetRows = await fileToRows(datasetFile);
  const datadicRows = await fileToRows(datadicFile);
  return {
    dataset: { rows: datasetRows },
    datadic: { rows: datadicRows },
  };
}

//async version
async function callAsyncValidation(
  datasetPath: string,
  datadicPath: string,
): Promise<string> {
  const payload = await buildRequestBody(datasetPath, datadicPath);
  const resp = await axios.post(ASYNC_URL, payload);

  console.log("Create task status:", resp.status);

  if (resp.status !== 200) {
    throw new Error(`Error creating task: ${JSON.stringify(resp.data)}`);
  }

  const taskId = resp.data?.id;
  if (!taskId)
    throw new Error(`No task id returned: ${JSON.stringify(resp.data)}`);

  console.log(`Created task id: ${taskId}`);
  return taskId;
}

async function pollTask(
  taskId: string,
  interval = 1000,
  maxAttempts = 60,
): Promise<AnyDict> {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const url = ASYNC_STATUS_URL.replace("{task_id}", taskId);
    const resp = await axios.get(url);

    console.log(`[poll ${attempt}] status code:`, resp.status);

    if (resp.status === 404) {
      console.error("Task not found");
      return { error: "Task not found" };
    }

    const data = resp.data;
    const status = data.status;
    console.log("Task status:", status);

    if (status === "DONE" || status === "ERROR") {
      return data;
    }

    await new Promise((res) => setTimeout(res, interval));
  }

  return { error: "Polling timed out", task_id: taskId };
}

//sync version (don't use in prod)
async function callSyncValidation(
  datasetPath: string,
  datadicPath: string,
): Promise<AnyDict> {
  const payload = await buildRequestBody(datasetPath, datadicPath);
  const resp = await axios.post(SYNC_URL, payload);

  console.log("Sync call status:", resp.status);

  if (resp.status !== 200) {
    throw new Error(
      `Error from sync validate/core: ${JSON.stringify(resp.data)}`,
    );
  }

  return resp.data;
}

async function main() {
  const args = process.argv.slice(2);
  let [datasetPath, datadicPath, mode] = args;

  if (args.length < 3) {
    datasetPath = "../../data/incorrect_dataset.csv";
    datadicPath = "../../data/incorrect_data_dictionary.csv";
    mode = "async";
    console.log(
      `Usage: ts-node validateClient.ts <dataset_file> <datadic_file> <mode: async|sync>`,
    );
    console.log(`Using defaults: ${datasetPath}, ${datadicPath}, mode=${mode}`);
  }

  try {
    if (mode === "async") {
      const taskId = await callAsyncValidation(datasetPath!, datadicPath!);
      const result = await pollTask(taskId);
      console.log("Final task response JSON:");
      console.log(JSON.stringify(result, null, 2));
    } else if (mode === "sync") {
      const result = await callSyncValidation(datasetPath!, datadicPath!);
      console.log("Sync validation response JSON:");
      console.log(JSON.stringify(result, null, 2));
    } else {
      console.error(`Unknown mode '${mode}', use 'async' or 'sync'`);
    }
  } catch (err: any) {
    console.error("Error:", err.message);
  }
}

if (require.main === module) {
  main();
}
