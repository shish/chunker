#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


static char *hash_type = "sha256";


/*
 * Simple hasher, via openssl
 */
#include <openssl/evp.h>

char *hash2str(char *data, int data_len) {
	EVP_MD_CTX *mdctx;
	const EVP_MD *md;
	unsigned char md_value[EVP_MAX_MD_SIZE];
	unsigned int md_len;
	int n;

	OpenSSL_add_all_digests();

	md = EVP_get_digestbyname(hash_type);

	if(!md) {
		printf("Unknown message digest\n");
		exit(1);
	}

	mdctx = EVP_MD_CTX_create();
	EVP_DigestInit_ex(mdctx, md, NULL);
	EVP_DigestUpdate(mdctx, data, data_len);
	EVP_DigestFinal_ex(mdctx, md_value, &md_len);
	EVP_MD_CTX_destroy(mdctx);

	char *out = malloc(sizeof(char) * (md_len * 2) + 1);
	for (n = 0; n < md_len; ++n) {
        snprintf(&(out[n*2]), md_len*2, "%02x", (unsigned int)md_value[n]);
    }
	out[n*2] = '\0';
	return out;
}

void print_chunk(char *data, int start, int end) {
	int length = end - start;
	char *hashbuf = hash2str(data + start, length);
	printf("{'hash_type': '%s', 'length': %d, 'hash': '%s'},\n", hash_type, length, hashbuf);
	free(hashbuf);
}


/*
 * Rusty Russel's chunking algo from gzip --rsyncable
 *
 * break at offset n if sum(data[n-4096:n]) % 4096 == 0
 *
 * Gives an average block size around 4kb, with a lot
 * of variation (blocks of 1-5 bytes are common)
 */
void chunk_v1(char *data, int size) {
	int WINDOW_SIZE = 4096;

	int i = 0;
	int sum = 0;
	int offset = 0;
	int n = 0;

	for(i=0; i<size; i++) {
		sum = sum + data[i];
		if(i >= WINDOW_SIZE) {
			sum = sum - data[i - WINDOW_SIZE];
		}
		if(
			(
				sum % WINDOW_SIZE == 0
			) || (
				i == size - 1
			)
		) {
			print_chunk(data, offset, i);
			offset = i;
			n++;
		}
	}
	//printf("%d bytes / %d chunks = %d bytes per chunk\n", size, n, size / n);
}


/*
 * A version of Rusty's algorithm with a larger window
 * and min / max block sizes; still more variable than
 * I'd like
 */
void chunk_v2(char *data, int size) {
	int WINDOW_SIZE = (1024*4);
	int CHUNK_MIN = WINDOW_SIZE;
	int CHUNK_MAX = 1024*1024;

	int i = 0;
	int sum = 0;
	int offset = 0;
	int n = 0;

	for(i=0; i<size; i++) {
		sum = sum + data[i];
		if(i >= WINDOW_SIZE) {
			sum = sum - data[i - WINDOW_SIZE];
		}
		if(
			(
				(
					sum % WINDOW_SIZE == 0
				) && (
					i - offset >= CHUNK_MIN
				)
			) || (
				i - offset >= CHUNK_MAX
			) || (
				i == size - 1
			)
		) {
			print_chunk(data, offset, i);
			offset = i;
			n++;
		}
	}
	//printf("%d bytes / %d chunks = %d bytes per chunk\n", size, n, size / n);
}


/*
 * The rsync rolling checksum, approximately
 *
 * http://snipperize.todayclose.com/snippet/py/Rsync-Algorithm-In-Python--188001/
 *
 * chunk sizes are fairly reliably close to the target size
 */
long a=1, b=0;
long MOD_ADLER = 4294967291;
int V3_BLOCK_SIZE = 4096;

long adler64(char *data, int len) {
	int i;
	for(i=0; i<len; i++) {
		char e = data[i];
		a = (e + a) % MOD_ADLER;
		b = (b + a) % MOD_ADLER;
	}
	return (b << 32) | a;
}

long rolladler(char removed, char new) {
	a = (a - removed + new) % MOD_ADLER;
	b = (b - removed * V3_BLOCK_SIZE - 1 + a) % MOD_ADLER;
	return (b << 32) | a;
}

void chunk_v3(char *data, int size) {
	int TARGET_CHUNK_SIZE = 1024 * 1024;
	int n = 0;
	int offset = 0;
	int i;
	long sum = adler64(data, V3_BLOCK_SIZE);

	for(i=V3_BLOCK_SIZE; i<size; i++) {
		if((sum % TARGET_CHUNK_SIZE == 0) || (i == size - 1)) {
			print_chunk(data, offset, i);
			offset = i;
			n++;
		}
		sum = rolladler(data[i - V3_BLOCK_SIZE], data[i]);
	}
}


/*
 * The main thing: read a file, pass it to the chunking algorithm
 */
int main(int argc, char *argv[]) {
	/*
	 * Check args
	 */
	char *method = "1";

	if(argc == 1) {
		printf("Usage: %s <filename> [hash alg]\n", argv[0]);
		return 1;
	}
	if(argc >= 3) {
		hash_type = argv[2];
	}
	if(argc >= 4) {
		method = argv[3];
	}

	/*
	 * Get the file mmap'ed
	 */
	int fd;
	struct stat fdstat;
	char *memblock;

	fd = open(argv[1], O_RDONLY);
	fstat(fd, &fdstat);

	memblock = mmap(NULL, fdstat.st_size, PROT_WRITE, MAP_PRIVATE, fd, 0);
	if(memblock == MAP_FAILED) {
		printf("mmap failed\n");
		return 2;
	}

	/*
	 * Find the checksum
	 */
	if(strcmp(method, "1") == 0) {
		chunk_v1(memblock, fdstat.st_size);
	}
	if(strcmp(method, "2") == 0) {
		chunk_v2(memblock, fdstat.st_size);
	}
	if(strcmp(method, "3") == 0) {
		chunk_v3(memblock, fdstat.st_size);
	}
	return 0;
}

