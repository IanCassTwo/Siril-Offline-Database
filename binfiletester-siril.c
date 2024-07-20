/*
    SPDX-FileCopyrightText: 2008 Akarsh Simha <akarshsimha@gmail.com>

    SPDX-License-Identifier: GPL-2.0-or-later
*/

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <sys/types.h>
#include <string.h>
#include "byteorder.h"
#include <stdint.h>  

#define HTM_LEVEL        6

#include "binfile.h"

/*
 * struct to store star and flux data, to be written in this format, into the binary file.
 */

#pragma pack(push, 1)
typedef struct {
    uint64_t source_id;  // Source ID
    int32_t RA;          // RA * 1000000
    int32_t Dec;         // Dec * 100000
    int32_t dRA;         // Proper motion in RA * 1000
    int32_t dDec;        // Proper motion in Dec * 1000
    uint16_t mag;        // Magnitude (half precision float)
    uint8_t fexpo;       // Flux Exponent
    int16_t flux[343];  // Compressed flux (fixed-length array of 343 elements)
} starData;
#pragma pack(pop)

typedef struct {
    uint64_t source_id;  // Source ID
    float RA;            // RA * 1000000
    float Dec;           // Dec * 100000
    float dRA;           // Proper motion in RA * 1000
    float dDec;          // Proper motion in Dec * 1000
    float mag;           // Magnitude (half precision float)
    uint8_t fexpo;       // Flux Exponent
    float flux[343];     // Compressed flux (fixed-length array of 343 elements)
} starDataFloat;

typedef uint16_t half_precision_float;

dataElement de[500];
u_int16_t nfields;
long index_offset, data_offset;
char byteswap;
u_int32_t ntrixels;

/*
 * Does byteswapping for starData structures
 */
void bswap_stardata(starData *stardata)
{
    stardata->RA       = bswap_32(stardata->RA);
    stardata->Dec      = bswap_32(stardata->Dec);
    stardata->dRA      = bswap_32(stardata->dRA);
    stardata->dDec     = bswap_32(stardata->dDec);
    stardata->mag      = bswap_16(stardata->mag);
    // TODO byte swap for each flux element
}

int verifyIndexValidity(FILE *f)
{
    int i;
    u_int32_t trixel;
    u_int64_t offset;
    u_int64_t prev_offset;
    u_int32_t nrecs;
    u_int32_t prev_nrecs;
    unsigned int nerr;

    fprintf(stdout, "Performing Index Table Validity Check...\n");
    index_offset = ftell(f);
    fprintf(stdout, "Assuming that index starts at %X\n", index_offset);

    prev_offset = 0;
    prev_nrecs  = 0;
    nerr        = 0;

    for (i = 0; i < ntrixels; ++i)
    {
        if (!fread(&trixel, 4, 1, f))
        {
            fprintf(stdout, "Table truncated before expected! Read i = %d records so far\n", i);
            +nerr;
            break;
        }
        if (byteswap)
            trixel = bswap_32(trixel);
        if (trixel >= ntrixels)
        {
            fprintf(stdout, "Trixel number %u is greater than the expected number of trixels %u\n", trixel, ntrixels);
            ++nerr;
        }
        if (trixel != i)
        {
            fprintf(stdout, "Found trixel = %d, while I expected number = %d\n", trixel, i);
            ++nerr;
        }
        fread(&offset, 8, 1, f);
        if (byteswap)
            offset = bswap_64(offset);
        fread(&nrecs, 4, 1, f);
        if (byteswap)
            nrecs = bswap_32(nrecs);

        if (nrecs == 0)
            continue;

        if (prev_offset != 0 && prev_nrecs != (-prev_offset + offset) / sizeof(starData))
        {
            fprintf(stdout, "Expected %u  = (%X - %x) / %d records, but found %u, in trixel %d\n",
                    (offset - prev_offset) / sizeof(starData), offset, prev_offset, sizeof(starData), nrecs, trixel);
            ++nerr;
        }
        prev_offset = offset;
        prev_nrecs  = nrecs;
    }

    data_offset = ftell(f);

    if (nerr)
    {
        fprintf(stdout, "ERROR ;-): The index seems to have %u errors\n", nerr);
    }
    else
    {
        fprintf(stdout, "Index verified. PASSED.\n");
    }
}

static float half_to_float(const uint16_t val) {
    // Extract the sign from the bits
    const uint32_t sign = (uint32_t)(val & 0x8000) << 16;
    // Extract the exponent from the bits
    const uint8_t exp16 = (val & 0x7c00) >> 10;
    // Extract the fraction from the bits
    uint16_t frac16 = val & 0x3ff;

    uint32_t exp32 = 0;
    if (exp16 == 0x1f) {
        exp32 = 0xff;
    } else if (exp16 == 0) {
        exp32 = 0;
    } else {
        exp32 = (uint32_t)exp16 + 112;
    }

    // corner case: subnormal -> normal
    // The denormal number of FP16 can be represented by FP32, therefore we need
    // to recover the exponent and recalculate the fraction.
    if (exp16 == 0 && frac16 != 0) {
        uint8_t offset = 0;
        do {
            ++offset;
            frac16 <<= 1;
        } while ((frac16 & 0x400) != 0x400);
        // mask the 9th bit
        frac16 &= 0x3ff;
        exp32 = 113 - offset;
    }

    uint32_t frac32 = (uint32_t)frac16 << 13;

    // Compose the final FP32 binary
    uint32_t bits = 0;
    bits |= sign;
    bits |= (exp32 << 23);
    bits |= frac32;

    return *(float *)&bits;
}

void printScaledStarData(const starData* data) {
    printf("Source ID: %lu\n", data->source_id);
    printf("RA: %d\n", data->RA);
    printf("Dec: %d\n", data->Dec);
    printf("Proper Motion RA: %d\n", data->dRA);
    printf("Proper Motion Dec: %d\n", data->dDec);
    printf("Magnitude: %d\n", half_to_float(data->mag));
    printf("Exponent: %d\n", data->fexpo);
    for (int i = 0; i < 343; i++) {
        printf("Flux[%d]: %.15e\n", i, data->flux[i]);
    }
}

void printUnscaledStarData(const starDataFloat* data) {
    printf("Source ID: %lu\n", data->source_id);
    printf("RA: %f\n", data->RA);
    printf("Dec: %f\n", data->Dec);
    printf("Proper Motion RA: %f\n", data->dRA);
    printf("Proper Motion Dec: %f\n", data->dDec);
    printf("Magnitude: %f\n", data->mag);
    printf("Exponent: %d\n", data->fexpo);
    for (int i = 0; i < 343; i++) {
        printf("Flux[%d]: %.15e\n", i, data->flux[i]);
    }
}

void unscaleStarData(starData* data, starDataFloat* unscaled_data) {

    int exponent = data->fexpo;
    unscaled_data->source_id = data->source_id;
    unscaled_data->RA = data->RA / 1000000.0f;
    unscaled_data->Dec = data->Dec / 100000.0f;
    unscaled_data->dRA = data->dRA / 1000.0f;
    unscaled_data->dDec = data->dDec / 1000.0f;
    unscaled_data->mag = half_to_float(data->mag);
    unscaled_data->fexpo = exponent;
    for (int i = 0; i < 343; i++) {
        float d = half_to_float(data->flux[i]);
        unscaled_data->flux[i] = d / pow(10.0f, exponent);
    }
}

/**
 *This method ensures that the data part of the file is sane. It ensures that there are no jumps in magnitude etc.
 */
int verifyStarData(FILE *f)
{
    int16_t faintMag;
    int8_t HTM_Level;
    u_int16_t MSpT;
    int16_t realFaintMag = -500;
    u_int16_t realMSpT;
    u_int32_t rtrixel;
    u_int32_t nstars;
    u_int32_t offset;

    int trixel, i;
    int nerr_trixel;
    int nerr;

    starData data;
    int16_t mag;

    fprintf(stdout, "Assuming that the data starts at 0x%X\n", ftell(f));
    fread(&faintMag, 2, 1, f);
    fprintf(stdout, "Faint Magnitude Limit: %f\n", faintMag / 100.0);
    fread(&HTM_Level, 1, 1, f);
    fprintf(stdout, "HTMesh Level: %d\n", HTM_Level);
    if (HTM_Level != HTM_LEVEL)
    {
        fprintf(stdout,
                "ERROR: HTMesh Level in file (%d) and HTM_LEVEL in program (%d) differ. Please set the define "
                "directive for HTM_LEVEL correctly and rebuild\n.",
                HTM_Level, HTM_LEVEL);
        return 0;
    }
    fread(&MSpT, 2, 1, f);

    mag  = -500;
    nerr = 0;

    // Scan the file for magnitude jumps, etc.
    realMSpT = 0;
    for (trixel = 0; trixel < ntrixels; ++trixel)
    {
        mag         = -500;
        nerr_trixel = 0;
 
        fprintf(stdout, "Seeking to: 0x%X\n", index_offset + trixel * INDEX_ENTRY_SIZE + 4);

        fseek(f, index_offset + trixel * INDEX_ENTRY_SIZE + 4, SEEK_SET);
        fread(&offset, 8, 1, f);
        fread(&nstars, 4, 1, f);


        if (nstars == 0) {
            fprintf(stdout, "Nothing to see here at trixel #%d: \n", trixel);
            continue;
        }

        fprintf(stdout, "Checking trixel #%d: 0x%X %d \n", trixel, offset, nstars);

        if (nstars > realMSpT)
            realMSpT = nstars;

        fseek(f, offset, SEEK_SET);
        for (i = 0; i < nstars; ++i)
        {
            fread(&data, sizeof(starData), 1, f);

            if (byteswap)
                bswap_stardata(&data);

            starDataFloat unscaled_data;

            //printScaledStarData(&data);
            unscaleStarData(&data, &unscaled_data );

            printUnscaledStarData(&unscaled_data);
            
            if (mag != -500 && ((unscaled_data.mag - mag) > 20 && mag < 1250) || unscaled_data.mag < mag)
            { // TODO: Make sensible magnitude limit (1250) user specifiable
                // TODO: Enable byteswapping
                fprintf(stdout, "\n\tEncountered jump of %f at star #%d in trixel %d from %f to %f.",
                        (unscaled_data.mag - mag), i, trixel, mag, unscaled_data.mag);
                ++nerr_trixel;
            }
            mag = unscaled_data.mag;
            if (mag > realFaintMag)
            {
                realFaintMag = mag;
            }
            if (mag > 1500)
                fprintf(stdout, "Magnitude > 15.00 ( = %f ) in trixel %d\n", mag / 100.0, trixel);
        }
        if (nerr_trixel > 0)
            fprintf(stdout, "\n * Encountered %d magnitude jumps in trixel %d\n", nerr_trixel, trixel);
        else
            fprintf(stdout, "Successful\n");
        nerr += nerr_trixel;
    }
    if (MSpT != realMSpT)
    {
        fprintf(stdout, "ERROR: MSpT according to file = %d, but turned out to be %d\n", MSpT, realMSpT);
        nerr++;
    }
    if (realFaintMag != faintMag)
    {
        fprintf(stdout, "ERROR: Faint Magnitude according to file = %f, but turned out to be %f\n", faintMag / 100.0,
                realFaintMag / 100.0);
        nerr++;
    }
    if (nerr > 0)
    {
        fprintf(stdout, "ERROR: Exiting with %d errors\n", nerr);
        return 0;
    }
    fprintf(stdout, "Data validation success!\n");
    return 1;
}


/*
void readStarList(FILE *f, u_int32_t trixel, FILE *names)
{
    long offset;
    long n;
    int i;
    u_int32_t nrecs;
    u_int32_t trix;
    char bayerName[8];
    char longName[32];
    starData data;

    printf("Reading star list for trixel %d\n", trixel);
    rewind(f);
    offset = index_offset +
             trixel * INDEX_ENTRY_SIZE; // CAUTION: Change if the size of each entry in the index table changes
    fseek(f, offset, SEEK_SET);
    fread(&trix, 4, 1, f);
    if (byteswap)
        trix = bswap_32(trix);
    if (trix != trixel)
    {
        fprintf(
            stdout,
            "ERROR: Something fishy in the index. I guessed that %d would be here, but instead I find %d. Aborting.\n",
            trixel, trix);
        return;
    }
    fread(&offset, 4, 1, f);
    if (byteswap)
        offset = bswap_32(offset);
    fread(&nrecs, 4, 1, f);
    if (byteswap)
        nrecs = bswap_32(nrecs);

    if (fseek(f, offset, SEEK_SET))
    {
        fprintf(stdout,
                "ERROR: Could not seek to position %X in the file. The file is either truncated or the indexes are "
                "bogus.\n",
                offset);
        return;
    }
    printf("Data for trixel %d starts at offset 0x%X and has %d records\n", trixel, offset, nrecs);

    for (i = 0; i < nrecs; ++i)
    {
        offset = ftell(f);
        n      = (offset - data_offset) / sizeof(starData);
        fread(&data, sizeof(starData), 1, f);
        if (byteswap)
            bswap_stardata(&data);
        printf("Entry #%d\n", i);
        printf("\tRA = %f\n", data.RA / 1000000.0);
        printf("\tDec = %f\n", data.Dec / 100000.0);
        printf("\tdRA/dt = %f\n", data.dRA / 10.0);
        printf("\tdDec/dt = %f\n", data.dDec / 10.0);
        printf("\tParallax = %f\n", data.parallax / 10.0);
        printf("\tHD Catalog # = %lu\n", data.HD);
        printf("\tMagnitude = %f\n", data.mag / 100.0);
        printf("\tB-V Index = %f\n", data.bv_index / 100.0);
        printf("\tSpectral Type = %c%c\n", data.spec_type[0], data.spec_type[1]);
        printf("\tHas a name? %s\n", ((data.flags & 0x01) ? "Yes" : "No"));
        //
        //  if(data.flags & 0x01 && names) {
        //  fseek(names, n * (32 + 8) + 0xA0, SEEK_SET);
        //  fread(bayerName, 8, 1, names);
        //  fread(longName, 32, 1, names);
        //  printf("\t\tBayer Designation = %s\n", bayerName);
        //  printf("\t\tLong Name = %s\n", longName);
        //  }
        //
        printf("\tMultiple Star? %s\n", ((data.flags & 0x02) ? "Yes" : "No"));
        printf("\tVariable Star? %s\n", ((data.flags & 0x04) ? "Yes" : "No"));
    }
}
*/

/**
 *@short  Read the KStars binary file header and display its contents
 *@param f  Binary file to read from
 *@returns  non-zero if successful, zero if not
 */

int readFileHeader(FILE *f)
{
    int i;
    int16_t endian_id;
    char ASCII_text[125];
    u_int8_t version_no;

    if (f == NULL)
        return 0;

    fread(ASCII_text, 124, 1, f);
    ASCII_text[124] = '\0';
    printf("%s", ASCII_text);

    fread(&endian_id, 2, 1, f);
    if (endian_id != 0x4B53)
    {
        fprintf(stdout, "Byteswapping required\n");
        byteswap = 1;
    }
    else
    {
        fprintf(stdout, "Byteswapping not required\n");
        byteswap = 0;
    }

    fread(&version_no, 1, 1, f);
    fprintf(stdout, "File version number: %d\n", version_no);
    fread(&nfields, 2, 1, f);
    if (byteswap)
        nfields = bswap_16(nfields);
    fprintf(stdout, "%d fields reported\n", nfields);

    for (i = 0; i < nfields; ++i)
    {
        fread(&(de[i]), sizeof(struct dataElement), 1, f);
        if (byteswap)
            de->scale = bswap_32(de->scale);
        displayDataElementDescription(&(de[i]));
    }

    fread(&ntrixels, 4, 1, f);
    if (byteswap)
        ntrixels = bswap_32(ntrixels);
    fprintf(stdout, "Number of trixels reported = %d\n", ntrixels);

    return 1;
}

int main(int argc, char *argv[])
{
    FILE *f, *names;
    int16_t maglim = -500;
    names          = NULL;
    if (argc <= 1)
    {
        fprintf(stdout, "USAGE: %s filename [trixel]\n", argv[0]);
        fprintf(stdout, "Designed for use only with KStars star data files\n");
        return 1;
    }

    f = fopen(argv[1], "rb");

    if (f == NULL)
    {
        fprintf(stdout, "ERROR: Could not open file %s for binary read.\n", argv[1]);
        return 1;
    }

    readFileHeader(f);

    verifyIndexValidity(f);
    verifyStarData(f);

    //    fread(&maglim, 2, 1, f);
    //    fprintf(stdout, "Limiting Magnitude of Catalog File: %f\n", maglim / 100.0);

    if (argc > 2)
    {
        /*
          if(argc > 3)
          names = fopen(argv[3], "rb");
          else
          names = NULL;
        
          fprintf(stdout, "Names = %s\n", ((names)?"Yes":"No"));
        */
    //    rewind(f);
    //    readStarList(f, atoi(argv[2]), names);
    }

    fclose(f);
    if (names)
        fclose(names);

    return 0;
}
