/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.webserver;

import com.microsoft.reef.util.OSUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

/**
 * utility to run command
 */
public class CommandUtility {
    public static String runCommand(final String command) {
        final StringBuilder sb = new StringBuilder();
        try {
            String cmd = OSUtils.isWindows() ? "cmd.exe /c " + command : command;
            final Process proc = Runtime.getRuntime().exec(cmd);

            try (final BufferedReader input =
                         new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
                String line;
                while ((line = input.readLine()) != null) {
                    sb.append(line).append('\n');
                }
            }
        } catch (IOException ex) {
            sb.append(ex);
        }
        return sb.toString();
    }
}
