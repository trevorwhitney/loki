"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const core_1 = require("@actions/core");
const github_1 = require("@actions/github");
try {
    // `who-to-greet` input defined in action metadata file
    const nameToGreet = (0, core_1.getInput)("who-to-greet");
    console.log(`Hello ${nameToGreet}!`);
    const time = new Date().toTimeString();
    (0, core_1.setOutput)("time", time);
    // Get the JSON webhook payload for the event that triggered the workflow
    const payload = JSON.stringify(github_1.context.payload, undefined, 2);
    console.log(`The event payload: ${payload}`);
}
catch (error) {
    (0, core_1.setFailed)(error.message);
}
//# sourceMappingURL=hello-world.js.map