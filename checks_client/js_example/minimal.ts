import axios from "axios";

const API_BASE = "http://localhost:8000";
const ASYNC_URL = `${API_BASE}/v1/validate`;
const ASYNC_STATUS_URL = `${API_BASE}/v1/validate/{task_id}`;

//do whatever need to convert into json request body
type AnyDict = Record<string, any>;

async function buildRequestBody(
  datasetRows: string,
  datadicRows: string,
): Promise<AnyDict> {
  return {
    dataset: { rows: datasetRows },
    datadic: { rows: datadicRows },
  };
}

//send to API endpoint
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

//repeat query until return complete, max 5 attempts
async function pollTask(
  taskId: string,
  interval = 1000,
  maxAttempts = 5,
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

async function main() {
  try {
    const taskId = await callAsyncValidation("", "");
    const result = await pollTask(taskId);

    console.log("Final task response JSON:");
    console.log(JSON.stringify(result, null, 2));
  } catch (err: any) {
    console.error("Error:", err.message);
  }
}

if (require.main === module) {
  main();
}
