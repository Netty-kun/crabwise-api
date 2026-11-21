import { beforeEach, describe, expect, it, vi } from 'vitest';

const MEDIA_ID = 'media-1';
const USER_ID = 'user-1';

const { prismaMock, envMock, enqueueMock, objectExistsMock, s3UploadMock, s3DownloadMock, s3DeleteMock } = vi.hoisted(() => ({
  prismaMock: {
    media: {
      findUnique: vi.fn(),
      findMany: vi.fn(),
      create: vi.fn(),
      deleteMany: vi.fn(),
    },
    artworkMedia: { create: vi.fn() },
    portfolioMedia: { create: vi.fn() },
  },
  envMock: {
    s3Bucket: 'test-bucket',
    s3Region: 'us-east-1',
    isProduction: false,
    verifyS3Upload: undefined as boolean | undefined,
    redisUrl: 'redis://localhost:6379',
  },
  enqueueMock: vi.fn(),
  objectExistsMock: vi.fn(),
  s3UploadMock: vi.fn(),
  s3DownloadMock: vi.fn(),
  s3DeleteMock: vi.fn(),
}));

vi.mock('@/config', () => ({ env: envMock }));
vi.mock('@/services', () => ({ prisma: prismaMock }));
vi.mock('@/queues', () => ({ enqueueImageProcessing: enqueueMock }));
vi.mock('./s3.service', () => ({
  buildObjectKey: vi.fn(
    (userId: string, folder: string, name: string) => `${folder}/${userId}/${name}`,
  ),
  buildPublicUrl: vi.fn((key: string) => `https://cdn/${key}`),
  createPresignedUploadUrl: vi.fn(),
  objectExists: objectExistsMock,
  downloadObjectBuffer: s3DownloadMock,
  uploadObjectBuffer: s3UploadMock,
  deleteObject: s3DeleteMock,
}));
vi.mock('sharp', () => ({
  default: vi.fn(() => ({
    resize: vi.fn().mockReturnThis(),
    webp: vi.fn().mockReturnThis(),
    toBuffer: vi.fn().mockResolvedValue({
      data: Buffer.from('thumb'),
      info: { width: 300, height: 200 },
    }),
  })),
}));

import { confirmMediaUpload, deleteMedia } from './media.service';
import { processImage } from '../workers/media.worker';
import type { Job } from 'bullmq';
import type { ImageProcessingJobData } from '../queues/media.queue';

function makeMedia(overrides: Record<string, unknown> = {}) {
  return {
    id: MEDIA_ID,
    userId: USER_ID,
    type: 'IMAGE',
    variantOfId: null,
    bucket: 'test-bucket',
    key: 'images/user-1/file.jpg',
    mimeType: 'image/jpeg',
    size: BigInt(1024),
    width: null,
    height: null,
    createdAt: new Date(),
    ...overrides,
  };
}

function makeJob(): Job<ImageProcessingJobData> {
  return {
    id: 'job-1',
    data: { mediaId: MEDIA_ID },
    attemptsMade: 0,
    attemptsStarted: 0,
  } as unknown as Job<ImageProcessingJobData>;
}

beforeEach(() => {
  vi.clearAllMocks();
  envMock.isProduction = false;
  envMock.verifyS3Upload = undefined;
  prismaMock.media.findUnique.mockResolvedValue(makeMedia());
  prismaMock.media.findMany.mockResolvedValue([]);
  prismaMock.media.create.mockResolvedValue({});
  enqueueMock.mockResolvedValue(undefined);
  objectExistsMock.mockResolvedValue(true);
  s3DownloadMock.mockResolvedValue(Buffer.from('original-image'));
  s3UploadMock.mockResolvedValue(undefined);
});

describe('confirmMediaUpload S3 verification (#873)', () => {
  it('verifies the object exists before enqueueing when enabled', async () => {
    envMock.verifyS3Upload = true;

    await confirmMediaUpload(USER_ID, MEDIA_ID);

    expect(objectExistsMock).toHaveBeenCalledWith('images/user-1/file.jpg');
    expect(enqueueMock).toHaveBeenCalledWith(MEDIA_ID);
  });

  it('returns a clear error and skips enqueueing when the object is missing', async () => {
    envMock.verifyS3Upload = true;
    objectExistsMock.mockResolvedValue(false);

    await expect(confirmMediaUpload(USER_ID, MEDIA_ID)).rejects.toMatchObject({
      code: 'UNPROCESSABLE_ENTITY',
    });
    expect(enqueueMock).not.toHaveBeenCalled();
  });

  it('defaults to verifying in production without an explicit override', async () => {
    envMock.isProduction = true;

    await confirmMediaUpload(USER_ID, MEDIA_ID);

    expect(objectExistsMock).toHaveBeenCalled();
    expect(enqueueMock).toHaveBeenCalledWith(MEDIA_ID);
  });

  it('defaults to skipping verification outside production', async () => {
    await confirmMediaUpload(USER_ID, MEDIA_ID);

    expect(objectExistsMock).not.toHaveBeenCalled();
    expect(enqueueMock).toHaveBeenCalledWith(MEDIA_ID);
  });

  it('fails open on unexpected S3 errors rather than blocking confirm', async () => {
    envMock.verifyS3Upload = true;
    objectExistsMock.mockRejectedValue(new Error('S3 500'));

    await confirmMediaUpload(USER_ID, MEDIA_ID);

    expect(enqueueMock).toHaveBeenCalledWith(MEDIA_ID);
  });

  it('never verifies or enqueues for non-image media', async () => {
    envMock.verifyS3Upload = true;
    prismaMock.media.findUnique.mockResolvedValue(makeMedia({ type: 'DIGITAL_FILE' }));

    await confirmMediaUpload(USER_ID, MEDIA_ID);

    expect(objectExistsMock).not.toHaveBeenCalled();
    expect(enqueueMock).not.toHaveBeenCalled();
  });

  it('rejects confirmation for media the user does not own', async () => {
    await expect(confirmMediaUpload('someone-else', MEDIA_ID)).rejects.toMatchObject({
      code: 'FORBIDDEN',
    });
    expect(enqueueMock).not.toHaveBeenCalled();
  });
});

describe('media worker failure surfacing (#97)', () => {
  it('re-throws so BullMQ marks the job failed when the original download fails', async () => {
    s3DownloadMock.mockRejectedValue(new Error('S3 download failed'));

    await expect(processImage(makeJob())).rejects.toThrow('S3 download failed');
  });

  it('re-throws when sharp conversion fails', async () => {
    const sharpMock = (await import('sharp')).default as unknown as ReturnType<typeof vi.fn>;
    sharpMock.mockImplementation(() => {
      throw new Error('sharp: unsupported image format');
    });

    await expect(processImage(makeJob())).rejects.toThrow('sharp: unsupported image format');
  });

  it('re-throws when variant persistence (prisma.media.create) fails', async () => {
    prismaMock.media.create.mockRejectedValue(new Error('prisma write failed'));

    await expect(processImage(makeJob())).rejects.toThrow('prisma write failed');
  });

  it('re-throws when the S3 variant upload fails', async () => {
    s3UploadMock.mockRejectedValue(new Error('S3 upload failed'));

    await expect(processImage(makeJob())).rejects.toThrow('S3 upload failed');
  });

  it('does not throw when the media row is missing (skips gracefully)', async () => {
    prismaMock.media.findUnique.mockResolvedValue(null);

    await expect(processImage(makeJob())).resolves.toBeUndefined();
  });

  it('does not throw for non-image media (skips gracefully)', async () => {
    prismaMock.media.findUnique.mockResolvedValue(makeMedia({ type: 'DIGITAL_FILE' }));

    await expect(processImage(makeJob())).resolves.toBeUndefined();
  });

  it('completes without throwing on the happy path', async () => {
    await expect(processImage(makeJob())).resolves.toBeUndefined();

    // Both variants created: thumbnail + webp.
    expect(prismaMock.media.create).toHaveBeenCalledTimes(2);
  });
});

describe('deleteMedia ownership (#89)', () => {
  beforeEach(() => {
    s3DeleteMock.mockResolvedValue(undefined);
    prismaMock.media.deleteMany.mockResolvedValue({ count: 1 });
  });

  it('throws NOT_FOUND and deletes nothing when media is missing', async () => {
    prismaMock.media.findUnique.mockResolvedValue(null);

    await expect(deleteMedia(USER_ID, MEDIA_ID)).rejects.toMatchObject({
      code: 'NOT_FOUND',
    });
    expect(s3DeleteMock).not.toHaveBeenCalled();
    expect(prismaMock.media.deleteMany).not.toHaveBeenCalled();
  });

  it('throws FORBIDDEN and never touches storage or DB for a non-owner', async () => {
    await expect(deleteMedia('someone-else', MEDIA_ID)).rejects.toMatchObject({
      code: 'FORBIDDEN',
    });
    expect(s3DeleteMock).not.toHaveBeenCalled();
    expect(prismaMock.media.deleteMany).not.toHaveBeenCalled();
  });

  it('deletes the object, its variants and DB rows for the owner', async () => {
    prismaMock.media.findMany.mockResolvedValue([
      makeMedia({ id: 'variant-1', key: 'images/user-1/file-thumb.webp', variantOfId: MEDIA_ID }),
    ]);

    await deleteMedia(USER_ID, MEDIA_ID);

    expect(s3DeleteMock).toHaveBeenCalledWith('test-bucket', 'images/user-1/file.jpg');
    expect(s3DeleteMock).toHaveBeenCalledWith('test-bucket', 'images/user-1/file-thumb.webp');
    expect(prismaMock.media.deleteMany).toHaveBeenCalledTimes(1);
  });
});
