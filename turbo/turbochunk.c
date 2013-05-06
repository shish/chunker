#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>


static char *hash_type = "sha256";
int target_chunk_size = 512 * 1024;
int min_chunk_size = 0;
int max_chunk_size = 1024 * 1024 * 8;
int window_size = (1024*4);


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
	int i = 0;
	int sum = 0;
	int offset = 0;
	int n = 0;

	for(i=0; i<size; i++) {
		sum = sum + data[i];
		if(i >= window_size) {
			sum = sum - data[i - window_size];
		}
		if(((sum % window_size == 0) && (i - offset >= min_chunk_size)) || (i - offset >= max_chunk_size) || (i == size - 1)) {
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
	b = (b - removed * window_size - 1 + a) % MOD_ADLER;
	return (b << 32) | a;
}

void chunk_v2(char *data, int size) {
	int n = 0;
	int offset = 0;
	int i;
	long sum = adler64(data, window_size);

	for(i=window_size; i<size; i++) {
		if(((sum % target_chunk_size == 0) && (i - offset > min_chunk_size)) || (i == size - 1) || (i - offset > max_chunk_size)) {
			print_chunk(data, offset, i);
			offset = i;
			n++;
		}
		sum = rolladler(data[i - window_size], data[i]);
	}
}


/*
 * The main thing: read a file, pass it to the chunking algorithm
 */
int main(int argc, char *argv[]) {
	/*
	 * Check args
	 */
	int method = 1;
	char *filename = NULL;
	int c;

	while((c = getopt(argc, argv, "hm:a:")) != -1) {
		switch(c) {
			case 'h':
				printf("Usage: %s [opts] [filename]\n", argv[0]);
				printf("  -m METH    Select chunking method (1 or 2)\n");
				printf("  -a ALGO    Select hashing algo (md5, sha256, etc)\n");
				return 255;
			case 'm':
				method = atoi(optarg);
				break;
			case 'a':
				hash_type = optarg;
				break;
		}
	}
	filename = argv[optind];

	/*
	 * Get the file mmap'ed
	 */
	int fd;
	struct stat fdstat;
	char *memblock;

	fd = open(filename, O_RDONLY);
	fstat(fd, &fdstat);

	memblock = mmap(NULL, fdstat.st_size, PROT_WRITE, MAP_PRIVATE, fd, 0);
	if(memblock == MAP_FAILED) {
		printf("mmap failed\n");
		return 2;
	}

	/*
	 * Find the checksum
	 */
	switch(method) {
		case 1:
			chunk_v1(memblock, fdstat.st_size);
			break;
		case 2:
			chunk_v2(memblock, fdstat.st_size);
			break;
		default:
			printf("Invalid chunking method\n");
			return 3;
	}
	return 0;
}

