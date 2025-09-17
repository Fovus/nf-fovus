package nextflow.fovus.nio;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class FovusCliExecutor {

    public static class CliExecutionResult {
        private int exitCode;
        private String output;
        private String error;

        public CliExecutionResult(int exitCode, String output, String error) {
            this.exitCode = exitCode;
            this.output = output;
            this.error = error;
        }

        // Getters
        public int getExitCode() { return exitCode; }
        public String getOutput() { return output; }
        public String getError() { return error; }

        // Setters if needed
        public void setExitCode(int exitCode) { this.exitCode = exitCode; }
        public void setOutput(String output) { this.output = output; }
        public void setError(String error) { this.error = error; }
    }

    /**
     * Helper method to execute Fovus CLI commands
     * @param command the command to execute
     * @return CliExecutionResult
     */
    public static CliExecutionResult executeCommand(String command) {
        StringBuilder stdout = new StringBuilder();
        StringBuilder stderr = new StringBuilder();
        try {
            Process process = Runtime.getRuntime().exec(command);

            // Capture stdout
            BufferedReader outReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = outReader.readLine()) != null) {
                stdout.append(line).append(System.lineSeparator());
            }

            // Capture stderr
            BufferedReader errReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            while ((line = errReader.readLine()) != null) {
                stderr.append(line).append(System.lineSeparator());
            }

            int exitCode = process.waitFor();

            return new CliExecutionResult(exitCode, stdout.toString(), stderr.toString());

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            return new CliExecutionResult(-1, stdout.toString(), stderr.toString());
        }
    }
}
