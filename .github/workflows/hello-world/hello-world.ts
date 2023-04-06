import { getInput, setOutput, setFailed } from "@actions/core";
import { context } from "@actions/github";

try {
  // `who-to-greet` input defined in action metadata file
  const nameToGreet = getInput("who-to-greet");
  console.log(`Hello ${nameToGreet}!`);
  const time = new Date().toTimeString();
  setOutput("time", time);
  // Get the JSON webhook payload for the event that triggered the workflow
  const payload = JSON.stringify(context.payload, undefined, 2);
  console.log(`The event payload: ${payload}`);
} catch (error: any) {
  setFailed(error.message);
}
