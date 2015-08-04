/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dia.kafka.isatools.producer;

import org.json.simple.JSONObject;

import java.nio.file.*;

import static java.nio.file.StandardWatchEventKinds.*;
import static java.nio.file.LinkOption.*;

import java.nio.file.attribute.*;
import java.io.*;
import java.util.*;

/**
 * Example to watch a directory (or tree) for changes to files.
 */
public class DirWatcher {

    private final WatchService watcher;
    private final Map<WatchKey, Path> keys;

    /**
     * Creates a WatchService and registers the given directory
     *
     * @param dir path to be watched by the service
     */
    DirWatcher(Path dir) throws IOException {
        this.watcher = FileSystems.getDefault().newWatchService();
        this.keys = new HashMap<WatchKey, Path>();
        System.out.format("[%s] Scanning %s\n", ISAToolsKafkaProducer.class.getSimpleName(), dir);
        registerAll(dir);
        System.out.format("[%s] Done scanning %s.\n", ISAToolsKafkaProducer.class.getSimpleName(), dir);
    }

    @SuppressWarnings("unchecked")
    static <T> WatchEvent<T> cast(WatchEvent<?> event) {
        return (WatchEvent<T>) event;
    }

    /**
     * Register the given directory with the WatchService
     *
     * @param dir to be examine
     */
    private void register(Path dir) throws IOException {
        if (!dir.toFile().isHidden()) {
            WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
            Path prev = keys.get(key);
            if (prev == null) {
                System.out.format("[%s] Registering %s\n", ISAToolsKafkaProducer.class.getSimpleName(), dir);
            } else {
                if (!dir.equals(prev)) {
                    System.out.format("[%s] Update %s -> %s\n", ISAToolsKafkaProducer.class.getSimpleName(), prev, dir);
                }
            }
            keys.put(key, dir);
        }
    }

    /**
     * Register the given directory, and all its sub-directories, with the
     * WatchService.
     */
    private void registerAll(final Path start) throws IOException {
        // register directory and sub-directories
        Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                    throws IOException {
                register(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * Process all events for keys queued to the watcher
     * @param isatProd
     */
    void processEvents(ISAToolsKafkaProducer isatProd) {
        for (; ; ) {

            // wait for key to be signalled
            WatchKey key;
            try {
                key = watcher.take();
            } catch (InterruptedException x) {
                return;
            }

            Path dir = keys.get(key);
            if (dir == null) {
                System.err.println("WatchKey not recognized!!");
                continue;
            }


            List<JSONObject> jsonParsedResults = new ArrayList<JSONObject>();

            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind kind = event.kind();

                // TBD - provide example of how OVERFLOW event is handled
                if (kind == OVERFLOW) {
                    continue;
                }

                // Context for directory entry event is the file name of entry
                WatchEvent<Path> ev = cast(event);
                Path name = ev.context();
                Path child = dir.resolve(name);

                // If an inner file has been modify then, recreate the entry
                if (kind == ENTRY_MODIFY || kind == ENTRY_CREATE) {
                    File fileCheck = child.getParent().toFile();
                    if (child.toFile().isDirectory()) {
                        fileCheck = child.toFile();
                    }

                    System.out.format("[%s] %s : %s\n", this.getClass().getSimpleName(), kind.toString(), fileCheck.getAbsolutePath());
                    List<String> folderFiles = ISAToolsKafkaProducer.getFolderFiles(fileCheck);
                    List<JSONObject> jsonObjects = ISAToolsKafkaProducer.doTikaRequest(folderFiles);
                    if (!jsonObjects.isEmpty()) {
//                        jsonParsedResults.addAll(jsonObjects);
                        isatProd.sendISAToolsUpdates(jsonObjects);
                    }
                }

                // TODO this event has still to be specified for documents
                if (kind == ENTRY_DELETE) {
                    System.err.println(String.format("Delete event not supported %s", child.toAbsolutePath()));
                }

                // if directory is created, and watching recursively, then
                // register it and its sub-directories
                if (kind == ENTRY_CREATE) {
                    try {
                        if (Files.isDirectory(child, NOFOLLOW_LINKS)) {
                            registerAll(child);
                        }
                    } catch (IOException x) {
                        // ignore to keep sample readbale
                        System.err.format("IOException when creating %s \n", child.toAbsolutePath());
                    }
                }
            }

            // reset key and remove from set if directory no longer accessible
            boolean valid = key.reset();
            if (!valid) {
                keys.remove(key);

                // all directories are inaccessible
                if (keys.isEmpty()) {
                    break;
                }
            }
        }
    }
}