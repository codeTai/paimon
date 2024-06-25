/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.manifest;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileEntry.Identifier;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Metadata of a manifest file. */
public class ManifestFileMeta {

    private static final Logger LOG = LoggerFactory.getLogger(ManifestFileMeta.class);

    private final String fileName;
    private final long fileSize;
    private final long numAddedFiles;
    private final long numDeletedFiles;
    private final SimpleStats partitionStats;
    private final long schemaId;

    public ManifestFileMeta(
            String fileName,
            long fileSize,
            long numAddedFiles,
            long numDeletedFiles,
            SimpleStats partitionStats,
            long schemaId) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.numAddedFiles = numAddedFiles;
        this.numDeletedFiles = numDeletedFiles;
        this.partitionStats = partitionStats;
        this.schemaId = schemaId;
    }

    public String fileName() {
        return fileName;
    }

    public long fileSize() {
        return fileSize;
    }

    public long numAddedFiles() {
        return numAddedFiles;
    }

    public long numDeletedFiles() {
        return numDeletedFiles;
    }

    public SimpleStats partitionStats() {
        return partitionStats;
    }

    public long schemaId() {
        return schemaId;
    }

    public static RowType schema() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "_FILE_NAME", new VarCharType(false, Integer.MAX_VALUE)));
        fields.add(new DataField(1, "_FILE_SIZE", new BigIntType(false)));
        fields.add(new DataField(2, "_NUM_ADDED_FILES", new BigIntType(false)));
        fields.add(new DataField(3, "_NUM_DELETED_FILES", new BigIntType(false)));
        fields.add(new DataField(4, "_PARTITION_STATS", SimpleStatsConverter.schema()));
        fields.add(new DataField(5, "_SCHEMA_ID", new BigIntType(false)));
        return new RowType(fields);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ManifestFileMeta)) {
            return false;
        }
        ManifestFileMeta that = (ManifestFileMeta) o;
        return Objects.equals(fileName, that.fileName)
                && fileSize == that.fileSize
                && numAddedFiles == that.numAddedFiles
                && numDeletedFiles == that.numDeletedFiles
                && Objects.equals(partitionStats, that.partitionStats)
                && schemaId == that.schemaId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                fileName, fileSize, numAddedFiles, numDeletedFiles, partitionStats, schemaId);
    }

    @Override
    public String toString() {
        return String.format(
                "{%s, %d, %d, %d, %s, %d}",
                fileName, fileSize, numAddedFiles, numDeletedFiles, partitionStats, schemaId);
    }

    /**
     * Merge several {@link ManifestFileMeta}s. {@link ManifestEntry}s representing first adding and
     * then deleting the same data file will cancel each other.
     *
     * <p>NOTE: This method is atomic.
     */
    public static List<ManifestFileMeta> merge(
            List<ManifestFileMeta> input,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            int suggestedMinMetaCount,
            long manifestFullCompactionSize,
            RowType partitionType) {
        // these are the newly created manifest files, clean them up if exception occurs
        List<ManifestFileMeta> newMetas = new ArrayList<>();

        try {
            Optional<List<ManifestFileMeta>> fullCompacted =
                    tryFullCompaction(
                            input,
                            newMetas,
                            manifestFile,
                            suggestedMetaSize,
                            manifestFullCompactionSize,
                            partitionType);
            return fullCompacted.orElseGet(
                    () ->
                            tryMinorCompaction(
                                    input,
                                    newMetas,
                                    manifestFile,
                                    suggestedMetaSize,
                                    suggestedMinMetaCount));
        } catch (Throwable e) {
            // exception occurs, clean up and rethrow
            for (ManifestFileMeta manifest : newMetas) {
                manifestFile.delete(manifest.fileName);
            }
            throw new RuntimeException(e);
        }
    }

    private static List<ManifestFileMeta> tryMinorCompaction(
            List<ManifestFileMeta> input,
            List<ManifestFileMeta> newMetas,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            int suggestedMinMetaCount) {
        List<ManifestFileMeta> result = new ArrayList<>();
        List<ManifestFileMeta> candidates = new ArrayList<>();
        long totalSize = 0;
        // merge existing small manifest files
        //将多个小文件合并 合并的新文件存储到 result 以及 newMetas 中
        for (ManifestFileMeta manifest : input) {
            totalSize += manifest.fileSize;
            candidates.add(manifest);
            if (totalSize >= suggestedMetaSize) {
                // reach suggested file size, perform merging and produce new file
                mergeCandidates(candidates, manifestFile, result, newMetas);
                candidates.clear();
                totalSize = 0;
            }
        }

        // merge the last bit of manifests if there are too many
        //suggestedMinMetaCount 默认值为 manifest.merge-min-count 30 个,如果数量大于这个值则进行合并
        if (candidates.size() >= suggestedMinMetaCount) {
            mergeCandidates(candidates, manifestFile, result, newMetas);
        } else {
            result.addAll(candidates);
        }
        return result;
    }

    private static void mergeCandidates(
            List<ManifestFileMeta> candidates,
            ManifestFile manifestFile,
            List<ManifestFileMeta> result,
            List<ManifestFileMeta> newMetas) {
        if (candidates.size() == 1) {
            result.add(candidates.get(0));
            return;
        }

        Map<Identifier, ManifestEntry> map = new LinkedHashMap<>();
        FileEntry.mergeEntries(manifestFile, candidates, map);
        if (!map.isEmpty()) {
            List<ManifestFileMeta> merged = manifestFile.write(new ArrayList<>(map.values()));
            result.addAll(merged);
            newMetas.addAll(merged);
        }
    }

    public static Optional<List<ManifestFileMeta>> tryFullCompaction(
            List<ManifestFileMeta> inputs,
            List<ManifestFileMeta> newMetas,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            long sizeTrigger,
            RowType partitionType)
            throws Exception {
        // 1. should trigger full compaction
        //筛选出初步符合条件的 base 文件，尽量减少文件的读写, 判断标准就是文件大于suggestedMetaSize(默认是8MB)，并且其中不包含被删除的目录
        List<ManifestFileMeta> base = new ArrayList<>();
        int totalManifestSize = 0;
        int i = 0;
        for (; i < inputs.size(); i++) {
            ManifestFileMeta file = inputs.get(i);
            if (file.numDeletedFiles == 0 && file.fileSize >= suggestedMetaSize) {
                base.add(file);
                totalManifestSize += file.fileSize;
            } else {
                break;
            }
        }

        List<ManifestFileMeta> delta = new ArrayList<>();
        long deltaDeleteFileNum = 0;
        long totalDeltaFileSize = 0;
        // i 是继续上面逻辑的
        for (; i < inputs.size(); i++) {
            ManifestFileMeta file = inputs.get(i);
            delta.add(file);
            totalManifestSize += file.fileSize;
            deltaDeleteFileNum += file.numDeletedFiles();
            totalDeltaFileSize += file.fileSize();
        }
        // 进行触发提交的判断 阈值默认为 16mb 配置参数为 manifest.full-compaction-threshold-size
        if (totalDeltaFileSize < sizeTrigger) {
            return Optional.empty();
        }

        // 2. do full compaction

        LOG.info(
                "Start Manifest File Full Compaction, pick the number of delete file: {}, total manifest file size: {}",
                deltaDeleteFileNum,
                totalManifestSize);

        // 2.1. try to skip base files by partition filter
        // 收集 delta 中保存的 ManifestEntry ，FileEntry.mergeEntries 实现中会尽量合并 ManifestEntry，
        // 比如一个 ManifestEntry 有 add 类型 后面又出现了 delete 类型 那么这条记录就可以被抵消
        // 但是也存在另外一种情况 delta 保存的 只有 delete 类型，没有 add 类型，add 类型的记录其实是存储在前面的 base 文件中
        Map<Identifier, ManifestEntry> deltaMerged = new LinkedHashMap<>();
        //多线程读取，速度取决于读取的线程数 使用的 ForkJoinPool 保证了先后顺序
        FileEntry.mergeEntries(manifestFile, delta, deltaMerged);

        List<ManifestFileMeta> result = new ArrayList<>();
        int j = 0;
        //通过 partition 进行过滤，看看 deleteEntry 的分区信息是否存在于 base 存储的 manifest 中, 如果不涉及的直接添加到 result 集合中
        if (partitionType.getFieldCount() > 0) {
            Set<BinaryRow> deletePartitions = computeDeletePartitions(deltaMerged);
            Optional<Predicate> predicateOpt =
                    convertPartitionToPredicate(partitionType, deletePartitions);

            if (predicateOpt.isPresent()) {
                Predicate predicate = predicateOpt.get();
                for (; j < base.size(); j++) {
                    // TODO: optimize this to binary search.
                    ManifestFileMeta file = base.get(j);
                    if (predicate.test(
                            file.numAddedFiles + file.numDeletedFiles,
                            file.partitionStats.minValues(),
                            file.partitionStats.maxValues(),
                            file.partitionStats.nullCounts())) {
                        break;
                    } else {
                        result.add(file);
                    }
                }
            } else {
                // There is no DELETE Entry in Delta, Base don't need compaction
                j = base.size();
                result.addAll(base);
            }
        }

        // 2.2. try to skip base files by reading entries
        // 收集 deltaMerged 中 delete 类型的 entry 文件标识 存放到 deleteEntries 用于后面的判断
        Set<Identifier> deleteEntries = new HashSet<>();
        deltaMerged.forEach(
                (k, v) -> {
                    if (v.kind() == FileKind.DELETE) {
                        deleteEntries.add(k);
                    }
                });
        //查找 base 中最早匹配的 manifest 文件
        List<ManifestEntry> mergedEntries = new ArrayList<>();
        for (; j < base.size(); j++) {
            ManifestFileMeta file = base.get(j);
            boolean contains = false;
            for (ManifestEntry entry : manifestFile.read(file.fileName, file.fileSize)) {
                checkArgument(entry.kind() == FileKind.ADD);
                if (deleteEntries.contains(entry.identifier())) {
                    contains = true;
                } else {
                    mergedEntries.add(entry);
                }
            }
            if (contains) {
                // already read this file into fullMerged
                j++;
                break;
            } else {
                mergedEntries.clear();
                result.add(file);
            }
        }

        // 2.3. merge
        //创建滚动的 writer，目前只能创建一个 writer 一是顺序写 二是 保证滚动写 满足文件大小
        RollingFileWriter<ManifestEntry, ManifestFileMeta> writer =
                manifestFile.createRollingWriter();
        Exception exception = null;
        try {

            // 2.3.1 merge mergedEntries
            for (ManifestEntry entry : mergedEntries) {
                writer.write(entry);
            }
            //将剩余的 base 中的 manifest 文件写一遍 其中进行 deleteEntries 判断，在 deleteEntries 中的文件就不再写入
            // 2.3.2 merge base files
            for (Supplier<List<ManifestEntry>> reader :
                    FileEntry.readManifestEntries(manifestFile, base.subList(j, base.size()))) {
                for (ManifestEntry entry : reader.get()) {
                    checkArgument(entry.kind() == FileKind.ADD);
                    if (!deleteEntries.contains(entry.identifier())) {
                        writer.write(entry);
                    }
                }
            }
            //将 deltaMerged 为 add 类型的 manifestEntry 写出去
            // 2.3.3 merge deltaMerged
            for (ManifestEntry entry : deltaMerged.values()) {
                if (entry.kind() == FileKind.ADD) {
                    writer.write(entry);
                }
            }
        } catch (Exception e) {
            exception = e;
        } finally {
            if (exception != null) {
                IOUtils.closeQuietly(writer);
                throw exception;
            }
            writer.close();
        }

        List<ManifestFileMeta> merged = writer.result();
        //最终 result 包含两部分 一部分是 没有变动的 base 文件 一部分是新写入的 merged 文件
        result.addAll(merged);
        //newMetas 中包含的都是新写入的文件
        newMetas.addAll(merged);
        return Optional.of(result);
    }

    private static Set<BinaryRow> computeDeletePartitions(
            Map<Identifier, ManifestEntry> deltaMerged) {
        Set<BinaryRow> partitions = new HashSet<>();
        for (ManifestEntry manifestEntry : deltaMerged.values()) {
            if (manifestEntry.kind() == FileKind.DELETE) {
                BinaryRow partition = manifestEntry.partition();
                partitions.add(partition);
            }
        }
        return partitions;
    }

    private static Optional<Predicate> convertPartitionToPredicate(
            RowType partitionType, Set<BinaryRow> partitions) {
        Optional<Predicate> predicateOpt;
        if (!partitions.isEmpty()) {
            RowDataToObjectArrayConverter rowArrayConverter =
                    new RowDataToObjectArrayConverter(partitionType);

            List<Predicate> predicateList =
                    partitions.stream()
                            .map(rowArrayConverter::convert)
                            .map(values -> createPartitionPredicate(partitionType, values))
                            .collect(Collectors.toList());
            predicateOpt = Optional.of(PredicateBuilder.or(predicateList));
        } else {
            predicateOpt = Optional.empty();
        }
        return predicateOpt;
    }
}
