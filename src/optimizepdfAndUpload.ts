import * as path from "path";
import { writeFileSync, createWriteStream, existsSync, mkdirSync } from "fs";
import { pipeline } from "stream/promises";
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { Collection, MongoClient, ObjectId } from "mongodb";
import { Parser } from "json2csv";
import * as fs from "fs";
import mongoose from "mongoose";
import { exec } from "child_process";
import { promisify } from "util";

const execAsync = promisify(exec);

const AWS_CONFIG_ENV =
  "";
const ASSET_DB_NAME = "assets-db";
const LESSONPLAN_DB_NAME = "lessonplan-db";
const MONGO_URL =
  "";
const LESSONPLAN_DB_CONNECTION = `${MONGO_URL}${LESSONPLAN_DB_NAME}`;
const ASSET_DB_CONNECTION = `${MONGO_URL}${ASSET_DB_NAME}`;
const batchSize = 25;

// -- AWS Config
const AWS_CONFIG = JSON.parse(
  Buffer.from(AWS_CONFIG_ENV, "base64").toString("utf-8")
);
const s3 = new S3Client({
  region: AWS_CONFIG.aws.region,
  credentials: {
    accessKeyId: AWS_CONFIG.aws.accessKeyId,
    secretAccessKey: AWS_CONFIG.aws.secretAccessKey,
  },
});
const s3Bucket = "xseed-asset-vault-372-staging";

// -- Paths
const EXPORTS_DIR = path.join(__dirname, "..", "exports");
const CSV_PATH = path.join(EXPORTS_DIR, "compressed_assets_report.csv");
const TOTALS_PATH = path.join(EXPORTS_DIR, "compressed_assets_totals.csv");
const TMP_DIR = path.join(__dirname, "..", "temp");
let totalOriginalSize = 0;
let totalCompressedSize = 0;

if (!existsSync(EXPORTS_DIR)) mkdirSync(EXPORTS_DIR, { recursive: true });
if (!existsSync(TMP_DIR)) mkdirSync(TMP_DIR, { recursive: true });

// -- Helper
function getExtension(filename: string): string {
  return path.extname(filename).toLowerCase();
}

function getFileNameFromS3Key(key: string): string {
  return path.basename(key);
}

function getCompressedS3Key(originalS3Key: string) {
  return originalS3Key.replace(/\.pdf$/, "_compressed_optimized.pdf");
}

async function downloadFromS3(s3Key: string, destPath: string) {
  const command = new GetObjectCommand({ Bucket: s3Bucket, Key: s3Key });
  const { Body } = await s3.send(command);
  await pipeline(Body as any, createWriteStream(destPath));
}

async function uploadToS3(s3Key: string, filePath: string) {
  const fileStream = fs.createReadStream(filePath);
  const command = new PutObjectCommand({
    Bucket: s3Bucket,
    Key: s3Key,
    Body: fileStream,
  });
  await s3.send(command);
}

function extractAssetIds(obj: any): Set<string> {
  const assetIds = new Set<string>();

  function recurse(current: any) {
    if (typeof current === "object" && current !== null) {
      for (const key in current) {
        if (key === "assetId" && current[key]) {
          assetIds.add(current[key].toString());
        } else {
          recurse(current[key]);
        }
      }
    }
  }

  recurse(obj);
  return assetIds;
}

class DbConnectionImplementation {
  public lessonPlanDb: MongoClient;
  public assetsDb: MongoClient;
  constructor(lessonPlanDb: MongoClient, assetsDb: MongoClient) {
    this.lessonPlanDb = lessonPlanDb;
    this.assetsDb = assetsDb;
  }
}

class DbConnection {
  static instance: DbConnectionImplementation | undefined = undefined;

  constructor(lessonPlanDb: MongoClient, assetsDb: MongoClient) {
    if (!DbConnection.instance) {
      DbConnection.instance = new DbConnectionImplementation(
        lessonPlanDb,
        assetsDb
      );
    }
  }

  static get lessonPlanDbConnection() {
    return DbConnection.instance?.lessonPlanDb.db();
  }

  static get assetsDbConnection() {
    return DbConnection.instance?.assetsDb.db();
  }

  static get lessonPlanDbClient() {
    return DbConnection.instance?.lessonPlanDb;
  }

  static get assetsDbClient() {
    return DbConnection.instance?.assetsDb;
  }
}

async function establishMongodbConnection() {
  try {
    const lessonPlanDb: MongoClient = await MongoClient.connect(
      LESSONPLAN_DB_CONNECTION || ""
    );
    const assetsDb: MongoClient = await MongoClient.connect(
      ASSET_DB_CONNECTION || ""
    );
    new DbConnection(lessonPlanDb, assetsDb);
  } catch (error) {
    console.error("Error establishing MongoDB connection:", error);
    process.exit(1);
  }
}

class AssetRepository {
  collection: Collection | undefined = undefined;
  private static instance: AssetRepository;

  constructor() {
    if (AssetRepository.instance) {
      return AssetRepository.instance;
    }
    this.collection = DbConnection.assetsDbConnection?.collection("assets");
    AssetRepository.instance = this;
  }

  async getPdfAssetData(assetId: string): Promise<any> {
    const assetData = await this.collection?.findOne(
      { _id: new ObjectId(assetId), type: "pdf" },
      { projection: { data: 1 } }
    );
    return assetData;
  }

  async updateCompressedOptimizedUrl(
    assetId: string,
    compressedUrl: string
  ): Promise<void> {
    await this.collection?.updateOne(
      { _id: new ObjectId(assetId) },
      { $set: { "data.compressedOptimizedUrl": compressedUrl } }
    );
  }
}

class LessonPlanRepository {
  collection: Collection | undefined = undefined;
  private static instance: LessonPlanRepository;

  constructor() {
    if (LessonPlanRepository.instance) {
      return LessonPlanRepository.instance;
    }
    this.collection =
      DbConnection.lessonPlanDbConnection?.collection("lessonPlans");
    LessonPlanRepository.instance = this;
  }

  async getCountOfLessonPlans(): Promise<number> {
    const count = await this.collection?.countDocuments();
    return count || 0;
  }

  async getAllLessonPlans(lastLessonPlanId: mongoose.Types.ObjectId | null) {
    const query = lastLessonPlanId ? { _id: { $gt: lastLessonPlanId } } : {};
    const lessonPlanData = await this.collection
      ?.find(query)
      .sort({ _id: 1 })
      .limit(batchSize)
      .project({})
      .toArray();
    return {
      lessonPlanData: lessonPlanData,
      nextLessonPlanId:
        lessonPlanData?.[lessonPlanData.length - 1]?._id || null,
    };
  }
}

async function compressPdf(
  inputPath: string,
  outputPath: string
): Promise<void> {
  try {
    const command = `gs -dEmbedAllFonts=true -sDEVICE=pdfwrite -dCompatibilityLevel=1.4 -dDownsampleColorImages=true -dColorImageResolution=150 -dNOPAUSE -dQUIET -dBATCH -dAutoRotatePages=/None -sOutputFile="${outputPath}" "${inputPath}"`;
    await execAsync(command);
  } catch (error) {
    console.error(`Error compressing PDF: ${error.message}`);
    throw error;
  }
}

async function main() {
  await establishMongodbConnection();

  const csvHeaders = [
    "AssetId",
    "OriginalUrl",
    "OriginalAssetSize",
    "OptimizedAssetSize",
    "Status",
  ];
  const parser = new Parser({ fields: csvHeaders });
  const rows: any[] = [];

  const lessonPlanRepo = new LessonPlanRepository();
  const assetRepo = new AssetRepository();

  const countOfLessonPlans = await lessonPlanRepo.getCountOfLessonPlans();

  let batchesToProcessCounter = 0;
  let totalBatches = countOfLessonPlans / batchSize;
  let lessonPlanCounter = 0;
  let processedAssetCount = 0;
  let processingFailedAssetCount = 0;
  let lastLessonPlanId: mongoose.Types.ObjectId | null = null;

  while (lessonPlanCounter < countOfLessonPlans) {
    const { lessonPlanData, nextLessonPlanId } =
      await lessonPlanRepo.getAllLessonPlans(lastLessonPlanId);
    if (!lessonPlanData || lessonPlanData.length === 0) {
      console.log("No more lesson plans to process");
      break;
    }
    lastLessonPlanId = nextLessonPlanId;
    batchesToProcessCounter += 1;
    console.log(
      `currently processing batch ${batchesToProcessCounter}/${totalBatches}`
    );

    for (let lessonPlan of lessonPlanData) {
      lessonPlanCounter += 1;

      // Step 1: Extract assetIds
      const assetIds = extractAssetIds(lessonPlan);
      let assetCountInLp = 0;

      for (const assetId of assetIds) {
        try {
          const asset = await assetRepo.getPdfAssetData(assetId);

          const originalUrl = asset?.data?.original;

          if (!asset || !originalUrl) continue;
          // if(asset?.data?.compressedOptimizedUrl) {
          //   console.log(`✅ Asset ${assetId} already optimized`);
          //   continue;
          // }

          assetCountInLp++;

          // Step 3: Build mapping and derive file paths
          const compressedUrl = getCompressedS3Key(originalUrl);
          const originalFileName = getFileNameFromS3Key(originalUrl);
          const localOriginalPath = path.join(TMP_DIR, originalFileName);
          const compressedFileName = `${originalFileName.replace(
            ".pdf",
            ""
          )}_compressed_optimized.pdf`;
          const localCompressedPath = path.join(TMP_DIR, compressedFileName);

          // Step 4: Download from S3
          await downloadFromS3(originalUrl, localOriginalPath);

          // Step 5: Compress pdf
          await compressPdf(localOriginalPath, localCompressedPath);

          const originalSize = fs.statSync(localOriginalPath).size;
          const optimizedSize = fs.statSync(localCompressedPath).size;

          if (optimizedSize > originalSize) {
            console.log(
              `❌ Asset ${assetId} compression failed, size increased from ${originalSize} to ${optimizedSize}`
            );
            processingFailedAssetCount++;
          } else {
            // Step 6: Upload compressed pdf
            await uploadToS3(compressedUrl, localCompressedPath);

            // Step 7: Update asset document
            await assetRepo.updateCompressedOptimizedUrl(
              assetId,
              compressedUrl
            );

            // Step 8: Log in CSV
            totalOriginalSize += originalSize;
            totalCompressedSize += optimizedSize;
            rows.push({
              AssetId: assetId,
              OriginalUrl: originalUrl,
              OriginalAssetSize: `${(originalSize / (1024 * 1024)).toFixed(
                2
              )} MB`,
              OptimizedAssetSize: `${(optimizedSize / (1024 * 1024)).toFixed(
                2
              )} MB`,
              Status: "Done",
            });

            processedAssetCount++;

            console.log(`✅ Asset ${assetId} updated successfully`);
          }
          // Cleanup temp files
          fs.unlinkSync(localOriginalPath);
          fs.unlinkSync(localCompressedPath);
        } catch (error) {
          processingFailedAssetCount++;
          console.error(`Error processing asset ${assetId}:`, error);
          rows.push({
            AssetId: assetId,
            OriginalUrl: "",
            OriginalAssetSize: "",
            OptimizedAssetSize: "",
            Status: `Error: ${error.message}`,
          });
        }
      }

      console.log(
        `📘 Processed lesson plan ${lessonPlanCounter}/${countOfLessonPlans}`
      );
      console.log(`📘 Processed pdf asset count in this lp ${assetCountInLp}`);
      console.log(`📘 Processed total pdf asset count ${processedAssetCount}`);
      console.log(
        `📘 Processing failed total pdf asset count ${processingFailedAssetCount}`
      );
    }
  }

  // Write CSV
  const csv = parser.parse(rows);
  writeFileSync(CSV_PATH, csv + "\n");

  const totalsRow = [
    [
      "Total Original Size (MB)",
      (totalOriginalSize / (1024 * 1024)).toFixed(2),
    ],
    [
      "Total Compressed Size (MB)",
      (totalCompressedSize / (1024 * 1024)).toFixed(2),
    ],
    [
      "Total Saved (MB)",
      ((totalOriginalSize - totalCompressedSize) / (1024 * 1024)).toFixed(2),
    ],
    [
      "Compression Efficiency (%)",
      ((1 - totalCompressedSize / totalOriginalSize) * 100).toFixed(2),
    ],
  ];

  const totalsCsv = totalsRow.map((row) => row.join(",")).join("\n");
  writeFileSync(TOTALS_PATH, totalsCsv);

  console.log("Script completed!");
  console.log(`📘 Processed total pdf asset count ${processedAssetCount}`);
  console.log(
    `✅ Total Original Size: ${(totalOriginalSize / (1024 * 1024)).toFixed(
      2
    )} MB`
  );
  console.log(
    `✅ Total Compressed Size: ${(totalCompressedSize / (1024 * 1024)).toFixed(
      2
    )} MB`
  );
  console.log(
    `💾 Total Saved: ${(
      (totalOriginalSize - totalCompressedSize) /
      (1024 * 1024)
    ).toFixed(2)} MB`
  );

  process.exit(0);
}

main();
