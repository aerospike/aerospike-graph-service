/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

public class ExternalTest {
    private static class TCPClient {
        private final Socket clientSocket;
        private final PrintWriter out;
        private final BufferedReader in;

        private TCPClient(Socket clientSocket, BufferedReader in, PrintWriter out) {
            this.clientSocket = clientSocket;
            this.in = in;
            this.out = out;
        }

        public static TCPClient startConnection(String ip, int port) throws IOException {
            Socket clientSocket;
            PrintWriter out;
            BufferedReader in;
            clientSocket = new Socket(ip, port);
            out = new PrintWriter(clientSocket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            return new TCPClient(clientSocket, in, out);

        }

        public String sendMessage(String msg) throws IOException {
            out.println(msg);
            String resp = in.readLine();
            return resp;
        }

        public void stopConnection() throws IOException {
            in.close();
            out.close();
            clientSocket.close();
        }
    }

    public void benchmarkExternalEchoServer() throws IOException {
        List<Long> results = new ArrayList<>();
        final TCPClient client = TCPClient.startConnection("localhost", 1919);
        IntStream.range(0, 1000).forEach(i -> {
            long start = System.nanoTime();
            final String resp;
            try {
                resp = client.sendMessage("abc");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            assert resp.equals("abc");
            long finish = System.nanoTime();
            results.add(finish - start);
        });
        results.sort(new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return o2.compareTo(o1);
            }
        });
        System.out.println(results);
        final long average = results.stream().reduce((l1,l2)->l1+l2).get()/results.size();
        final long max = results.get(0);
        final long min = results.get(results.size()-1);
        System.out.println(String.format("delta ns, %d avg %d max %d min", average, max, min ));
    }

}
