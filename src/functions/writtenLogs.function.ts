import { FileHandle, mkdir, open } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import Path from "node:path";
import { TContent } from "../types/content.type";
import { crc32 } from "zlib";

/**
 * Varaibles config
 */
const LOOPS = 10_000_000;
const SEGMENT_LIMIT = 100 * 1024 * 1024;
const LOGS_DIR = Path.join(process.cwd(), "logs");

const HASH_MAP: Map<string, { segment: number; offset: number }> = new Map<
  string,
  { segment: number; offset: number }
>();

let currentSegmentId: number = 0;
let currentOffset: number = 0;
let currentFile: FileHandle;

/**
 * @method
 */
async function initSegment(): Promise<void> {
  await ensureLogsDir();
  currentFile = await open(Path.join(LOGS_DIR, `segment-${currentSegmentId}.log`), "a");
}
/**
 * @methdo
 */
async function rotateSegment(): Promise<void> {
  await currentFile.close();
  currentSegmentId++;
  currentOffset = 0;
  currentFile = await open(Path.join(LOGS_DIR, `segment-${currentSegmentId}.log`), "a");
}

/***
 * @method
 */
async function ensureLogsDir(): Promise<void> {
  await mkdir(LOGS_DIR, { recursive: true });
}

/**
 * @function writtenLogs
 * @description Write logs to a file
 * @returns {Promise<void>}
 */
export async function writtenLogs(): Promise<void> {
  const FORMATER = new Intl.DateTimeFormat("en-US", {
    month: "numeric",
    weekday: "long",
    year: "numeric",
  });
  await initSegment();
  try {
    for (let i = 0; i < LOOPS; i++) {
      const content: TContent = {
        id: randomUUID(),
        date: FORMATER.format(new Date()),
        timestamp: new Date().getTime(),
      };
      const recordBuffer = buildRecord(
        content.id,
        JSON.stringify(content),
        content.timestamp,
      );

      const RECORD_SIZE = recordBuffer.length;
      if (currentOffset + RECORD_SIZE > SEGMENT_LIMIT) {
        await rotateSegment();
      }
      const offset = currentOffset;
      await currentFile.write(recordBuffer);
      HASH_MAP.set(content.id, {
        segment: currentSegmentId,
        offset,
      });
      currentOffset += RECORD_SIZE;
    }
  } finally {
    await currentFile.close();
  }
}

/**
 * Build record
 */
function buildRecord(key: string, value: string, timestamp: number): Buffer {
  /**
   * Buffers
   */
  const keyBuffer = Buffer.from(key);
  const valueBuffer = Buffer.from(key);
  /**
   * Length buffer
   */
  const keySize = keyBuffer.length;
  const valueSize = valueBuffer.length;

  /**
   * Sizes
   */
  const HEADER_SIZE = 4 + 4 + 4 + 8;
  const TOTAL_SIZE = HEADER_SIZE + keySize + valueSize;

  const BUFFER: Buffer = Buffer.alloc(TOTAL_SIZE);
  let offset = 0;

  BUFFER.writeUInt32BE(keySize, offset);
  offset += 4;

  BUFFER.writeUInt32BE(valueSize, offset);
  offset += 4;

  BUFFER.writeBigUInt64BE(BigInt(timestamp), offset);
  offset += 8;

  valueBuffer.copy(BUFFER, offset);
  offset += valueSize;

  const _CRC32 = crc32(BUFFER.subarray(4)) >>> 0;
  BUFFER.writeUInt32BE(_CRC32, 0);

  return BUFFER;
}
