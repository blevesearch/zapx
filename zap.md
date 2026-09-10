# ZAP File Format

## Legend

### File Sections

    |========|
    |        | file section
    |========|

### Fixed-size fields

    |--------|        |----|        |--|        |-|
    |        | uint64 |    | uint32 |  | uint16 | | uint8
    |--------|        |----|        |--|        |-|

### Varints

    |~~~~~~~~|
    |        | varint(up to uint64)
    |~~~~~~~~|

### Arbitrary-length fields

    |--------...---|
    |              | arbitrary-length field (string, vellum, roaring bitmap)
    |--------...---|

### Chunked data

    [--------]
    [        ]
    [--------]

## Overview

Footer section describes the configuration of particular ZAP file. The format of footer is version-dependent, so it is necessary to check `V` field before the parsing.

            +=================================================================+
            | Stored Fields                                                   |
            |=================================================================|
    +-----> | Stored Fields Index                                             |
    |       |=================================================================|
    |       | Inverted Text Index Section                                     |
    |       |=================================================================|
    |       | Vector Index Section                                            |
    |       |=================================================================|
    |       | Synonym Index Section                                           |
    |       |=================================================================|
    |       | Sections Info                                                   |
    |       |=================================================================|
    |   +-> | Sections Index                                                  |
    |   |   |==..==+=======+======+======+=====+======+=====+======+==========|
    |   |   |  ID  |  IDL  |  D#  |  SF  |  S  |  CF  |  V  |  CC  | (Footer) |
    |   |   +==..==+=======+======+======+=====+======+=====+======+==========+
    |   |                             |     |
    +---------------------------------+     |
        |                                   |
        +-----------------------------------+

     ID. ID of the Writer Used.
    IDL. Length of the Writer ID.
     D#. Number of Docs.
     SF. Stored Fields Index Offset.
      S. Sections Index Offset
     CF. Chunk Factor.
      V. Version.
     CC. CRC32.

## Stored Fields

Stored Fields Index is `D#` consecutive 64-bit unsigned integers - offsets, where relevant Stored Fields Data records are located.
We also store the EdgeList for nested documents, if present in the segment, to preserve hierarchical relationships.
If there are NE edges, it means there are NE nested or sub-documents, with each edge representing a child -> parent relationship.

    0                                [SF]                   [SF + D# * 8]
    | Stored Fields                  | Stored Fields Index              | Edge List Information                                                  |
    |================================|==================================|========================================================================|
    |                                |                                  |                                                                        |
    |       |--------------------|   ||--------|--------|. . .|--------|||~~~~~~~~|~~~~~~~~|~~~~~~~~|~~~~~~~~|~~~~~~~~|. . .|~~~~~~~~~|~~~~~~~~~||
    |   |-> | Stored Fields Data |   ||      0 |      1 |     | D# - 1 |||   NE   |   C1   |   P1   |   C2   |   P2   |     |   CNE   |   PNE   ||
    |   |   |--------------------|   ||--------|----|---|. . .|--------|||~~~~~~~~|~~~~~~~~|~~~~~~~~|~~~~~~~~|~~~~~~~~|. . .|~~~~~~~~~|~~~~~~~~~||
    |   |                            |              |                   |                                                                        |
    |===|============================|==============|===================|========================================================================|

        NE. Number of edges in the edge list.
        Ci. Child Document Number for edge i.
        Pi. Parent Document Number for edge i.

Stored Fields Data is an arbitrary size record, which consists of metadata and [Snappy](https://github.com/golang/snappy)-compressed data.

    Stored Fields Data
    |~~~~~~~~|~~~~~~~~|~~~~~~~~...~~~~~~~~|~~~~~~~~...~~~~~~~~|
    |    MDS |    CDS |                MD |                CD |
    |~~~~~~~~|~~~~~~~~|~~~~~~~~...~~~~~~~~|~~~~~~~~...~~~~~~~~|

    MDS. Metadata size.
    CDS. Compressed data size.
    MD. Metadata.
    CD. Snappy-compressed data.

## Index Sections

Sections Index is a set of NF uint64 addresses (0 through F# - 1) each of which are offsets to the records in the Sections Info. Inside the sections info, we have further offsets to specific type of index section for that particular field in the segment file. For example, field 0 may correspond to Vector Indexing and its records would have offsets to the Vector Index Section whereas a field 1 may correspond to Text Indexing and its records would rather point to somewhere within the Inverted Text Index Section.

       (...)                                                                     [F]                          [F + F#]
       + Sections Info                                                             + Sections Index                  +
       |===========================================================================|=================================|
       |                                                                           |                                 |
       |  +--------+------+---+----+---------+---------+~~~~~+--+...+--+~~~~~~~~~+ | +------+------+...+------+----+ |
    +---->| Length | Name | O | NS | S1 Type | S1 Addr | ... | Sn Type | Sn Addr | | |    0 |    1 |   | F#-1 | NF | |
    |  |  +--------+------+---+----+---------+---------+~~~~~+--+...+--+~~~~~~~~~+ | +------+----+-+...+------+----+ |
    |  |                                                                           |             |                   |
    |  +===========================================================================+=============|===================+
    |                                                                                            |
    +--------------------------------------------------------------------------------------------+

     NF. Number of fields
     NS. Number of index sections
     O.  Field Indexing Options
     Sn. nth index section

## Inverted Text Index Section

Each field has its own types of indexes in separate sections as indicated above. This can be a vector index or inverted text index.

In case of inverted text index, the dictionary is encoded in [Vellum](https://github.com/couchbase/vellum) format. Dictionary consists of pairs `(term, offset)`, where `offset` indicates the position of the term header (described below) for this particular term.

Doc numbers and term frequencies are bitpacked 128 at a time. Doc numbers within a
postings list are strictly increasing, so `delta - 1` is stored, and a term present in
every document therefore packs at zero bits per document; term frequencies are at
least 1, so `tf - 1` is stored and a block of all-ones frequencies also costs nothing.
A flat skip list carries one fixed-size entry per full block, which lets a reader hop
to the block containing a target document without decoding any payload. Whatever is
left over at the end of a list -- fewer than 128 documents -- is written as plain
uvarints.

Field norms are **not** stored in the postings. They live in a dense column, one
quantized byte per document, shared by every term of the field: a term appearing in a
million documents would otherwise carry a million copies of the same handful of field
lengths. The quantization is the scheme Lucene and tantivy use -- exact for lengths
0..40, then a 3-bit mantissa with a 5-bit exponent -- so reading a norm is one byte
load plus one lookup in a 256-entry table.

        +================================================================+- Inverted Text
        |                                                                |  Index Section
        |                                                                |
        |    Field Norms                                                 |
        |    [~~~~~~+~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~]              |
        | +->[ Kind | absent | constant u8 | u8[numDocs] ]               |
        | |  [~~~~~~+~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~]              |
        | |                                                              |
        | |  Per term, in term order:                                    |
        | |                                                              |
        | |  Payload                                                     |
        | |  [~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~+~~~~~~~~~~~~~~~~~~~~~~]  |
        | |  [ 128 docNum deltas @docNumBits   | 128 tfs @tfNumBits  ]*  |
        | |  [~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~+~~~~~~~~~~~~~~~~~~~~~~]  |
        | |  [ uvarint doc deltas | uvarint tfs ]  (tail, < 128 docs)    |
        | |                                                              |
        | |  Location Details (chunked)                                  |
        | |  [~~~~~~+~~~~~+~~~~~~~+~~~~~+~~~~~~+~~~~~~~~+~~~~~]          |
        | |  [ Size | Pos | Start | End | Arr# | ArrPos | ... ]          |
        | |  [~~~~~~+~~~~~+~~~~~~~+~~~~~+~~~~~~+~~~~~~~~+~~~~~]          |
        | |                                                              |
        | |  Skip List (one entry per full block, then a tail offset)    |
        | |  [~~~~~~~~~+~~~~~~~~~~~~+~~~~~~~~~+~~~~~~~~+~~~~~~~+~~~~~~]  |
        | |  [ LastDoc | BlockOffset | DocBits | TfBits | MinNorm| MaxTf] |
        | |  [~~~~~~~~~+~~~~~~~~~~~~~+~~~~~~~~+~~~~~~~~+~~~~~~~~+~~~~~~] |
        | |                                                              |
        | |  Term Header                          <- the FST value       |
        | |  +~~~~~~~~~+~~~~~~~+~~~~~~~~~~~+~~~~~~~~~+~~~~~~~~~+~~~~~~+  |
        | +->| DocFreq | Flags | PayloadLen | LocsLen | SkipLen | LocCS| |
        | |  +~~~~~~~~~+~~~~~~~+~~~~~~~~~~~+~~~~~~~~~+~~~~~~~~~+~~~~~~+  |
        | |                                                              |
        | +----------------------------------------------+               |
        |                                                |               |
        |                     Dictionary                 |               |
        | +~~~~~~~~~~+~~~~~~~+~~~~~~~~+-------------------+-...-+~~~~~~~+ |
    +-----> DV Start | DV End| Length | VELLUM (TERM->OFFSET) | Norms  | |
    |   | +~~~~~~~~~~+~~~~~~~+~~~~~~~~+-------------------+-...-+~~~~~~~+ |
    |   |                                            Offset & Length      |
    |   |                                                                |
    |   |================================================================+- Vector Index Section
    |   |                                                                |
    |   +================================================================+- Synonym Index Section
    |   |                                                                |
    |   |================================================================+- Sections Info
    +-----------------------------+                                      |
        |                         |                                      |
        |     +-------+-----+-----+------+~~~~~~~~+~~~~~~~~+--+...+--+   |
        |     |  ...  | ITI | ITI ADDR   |   NS   | Length |    Name |   |
        |     +-------+-----+------------+~~~~~~~~+~~~~~~~~+--+...+--+   |
        +================================================================+


The regions of a term are written in the order the writer can produce them and the
header comes last, so nothing needs buffering or back-patching. A reader walks
backwards from the header: skip data starts at `header - skipLen`, locations at
`skip - locsLen`, payload at `locs - payloadLen`.

Each block is passed through the writer's `process()` hook independently, which is why
the skip entry records the block's byte offset rather than deriving it from the bit
widths: an encrypting or compressing callback changes a block's length, and a reader
must still be able to reach one block without decoding the whole term.

`MinNorm` and `MaxTf` bound the BM25 contribution of every document in a block --
score rises with frequency and falls with field length. Nothing consumes them yet;
they are written so that enabling block-max WAND later is not another format change.

A term with a single document, a frequency of one and no locations skips all of this:
its doc number is packed directly into the FST value ("1-hit" encoding). That is most
of the dictionary in a real corpus.

         ITI - Inverted Text Index

## Vector Index Section

In a vector index, each vector is assigned a unique, monotonically increasing ID ranging from `0` to `N-1`, where `N` is the total number of vectors in the index. This ID is used internally by the [Faiss](https://github.com/blevesearch/faiss) index. Each vector ID maps to a document ID within the segment, and this mapping is stored as an array of size `N`.

        |================================================================+- Inverted Text Index Section
        |                                                                |
        |================================================================+- Vector Index Section
        |                                                                |
        |   +~~~~~~~~~~+~~~~~~~~+~~~~~+~~~~~~+~~~~~~+                    |
    +-------> DV Start | DV End | VIO | NVEC |  ML  |                    |
    |   |   +~~~~~~~~~~+~~~~~~~~+~~~~~+~~~~~~+~~~~~~+                    |
    |   |                                                                |
    |   |   +~~~~~~~~~~~~~+                                              |
    |   |   |   DocID_1   |                                              |
    |   |   +~~~~~~~~~~~~~+                                              |
    |   |   |   DocID_2   |                                              |
    |   |   +~~~~~~~~~~~~~+                                              |
    |   |   |     ...     |                                              |
    |   |   +~~~~~~~~~~~~~+                                              |
    |   |   |   DocID_N   |                                              |
    |   |   +~~~~~~~~~~~~~+                                              |
    |   |                                                                |
    |   |   +~~~~~~~~~~~~~+                                              |
    |   |   |  INDEX TYPE |                                              |
    |   |   +~~~~~~~~~~~~~+                                              |
    |   |   +~~~~~~~~~~~~~+                                              |
    |   |   |  INDEX DATA |                                              |
    |   |   +~~~~~~~~~~~~~+                                              |
    |   |                                                                |
    |   |================================================================+- Synonym Index Section
    |   |                                                                |
    |   |================================================================+- Sections Info
    +-----------------------------+                                      |
        |                         |                                      |
        |     +-------+-----+-----+------+~~~~~~~~+~~~~~~~~+--+...+--+   |
        |     |  ...  | VI  | VI ADDR    |   NS   | Length |    Name |   |
        |     +-------+-----+------------+~~~~~~~~+~~~~~~~~+--+...+--+   |
        +================================================================+

         VI   - Vector Index
         VIO  - Vector Index Optimized for
         NVEC - Number of vectors
         ML   - Length of the vector to document ID map
         INDEX TYPE - Type of the vector index
         INDEX DATA - Vector index data

### Vector Index Type - FP32

FP32 vector indexes stores a singular FAISS index to perform search

    |   +~~~~~~~~~~~~~~~~~~~~~~~~~~+    |
    |   |        FAISS LEN         |    |
    |   +~~~~~~~~~~~~~~~~~~~~~~~~~~+    |
    |                                   |
    |   +----------+...+-----------+    |
    |   |  SERIALIZED FAISS INDEX  |    |
    |   +----------+...+-----------+    |

         FAISS LEN   - Length of the serialized faiss index

### Vector Index Type - Binary

Binary vector indexes stores two separate FAISS indexes to perform search. The first is a primary binary index and the second is a backing FP32 index

    |   +~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~+    |
    |   |        PRIMARY FAISS LEN         |    |
    |   +~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~+    |
    |                                           |
    |   +---------------+...+--------------+    |
    |   |  SERIALIZED PRIMARY FAISS INDEX  |    |
    |   +---------------+...+--------------+    |
    |                                           |
    |   +~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~+    |
    |   |        BACKING FAISS LEN         |    |
    |   +~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~+    |
    |                                           |
    |   +---------------+...+--------------+    |
    |   |  SERIALIZED BACKING FAISS INDEX  |    |
    |   +---------------+...+--------------+    |

         PRIMARY FAISS LEN   - Length of the serialized primary faiss index
         BACKING FAISS LEN   - Length of the serialized backing faiss index

## Synonym Index Section

In a synonyms index, the relationship between a term and its synonyms is represented using a Thesaurus. The Thesaurus is encoded in the [Vellum](https://github.com/couchbase/vellum) format and consists of pairs in the form `(term, offset)`. Here, the offset specifies the position of the postings list containing the synonyms for the given term. The postings list is stored as a Roaring64 bitmap, with each entry representing an encoded synonym for the term.

        |================================================================+- Inverted Text Index Section
        |                                                                |
        |================================================================+- Vector Index Section
        |                                                                |
        +================================================================+- Synonym Index Section
        |                                                                |
        |    (Offset)  +~~~~~+----------+...+---+                        |
        |   +--------->|  RL | ROARING64 BITMAP |                        |
        |   |          +~~~~~+----------+...+---+                        +------------------------+
        |   |(Term -> Offset)                                                                     |
        |   |                                                                                     |
        |   +--------+                                                                            |
        |            |                            Term ID to Term map (NST Entries)               |
        |    +~~~~+~~~~+~~~~~+~~~~[{~~~~~+~~~~+~~~~~~}{~~~~~+~~~~+~~~~~~}...{~~~~~+~~~~+~~~~~~}]  |
        | +->| VL | VD | NST | ML || TID | TL | Term || TID | TL | Term |   | TID | TL | Term |   |
        | |  +~~~~+~~~~+~~~~~+~~~~[{~~~~~+~~~~+~~~~~~}{~~~~~+~~~~+~~~~~~}...{~~~~~+~~~~+~~~~~~}]  |
        | |                                                                                       |
        | +----------------------------+                                                          |
        |                              |                                                          |
        | +~~~~~~~~~~+~~~~~~~~+~~~~~~~~~~~~~~~~~+                                                 |
    +-----> DV Start | DV End | ThesaurusOffset |                                                 |
    |   | +~~~~~~~~~~+~~~~~~~~+~~~~~~~~~~~~~~~~~+                        +------------------------+
    |   |                                                                |
    |   |                                                                |
    |   |================================================================+- Sections Info
    +-----------------------------+                                      |
        |                         |                                      |
        |     +-------+-----+-----+------+~~~~~~~~+~~~~~~~~+--+...+--+   |
        |     |  ...  | SI  | SI ADDR    |   NS   | Length |    Name |   |
        |     +-------+-----+------------+~~~~~~~~+~~~~~~~~+--+...+--+   |
        +================================================================+

         SI  - Synonym Index
         VL  - Vellum Length
         VD  - Vellum Data (Term -> Offset)
         RL  - Roaring64 Length
         NST - Number of entries in the term ID to term map
         ML  - Length of the term ID to term map
         TID - Term ID (32-bit)
         TL  - Term Length

### Synonym Encoding

        ROARING64 BITMAP

        Each 64-bit entry consists of two parts: the first 32 bits represent the Term ID (TID),
        and the next 32 bits represent the Document Number (DN).

        [{~~~~~+~~~~}{~~~~~+~~~~}...{~~~~~+~~~~}]
         | TID | DN || TID | DN |   | TID | DN |
        [{~~~~~+~~~~}{~~~~~+~~~~}...{~~~~~+~~~~}]

            TID - Term ID (32-bit)
            DN  - Document Number (32-bit)

## Doc Values

DocValue start and end offsets are stored within the section content of each field. This allows each field having its own type of index to choose whether to store the doc values or not. For example, it may not make sense to store doc values for vector indexing and so, the offsets can be invalid ones for it whereas the fields having text indexing may have valid doc values offsets.

    +================================================================+
    |     +------...--+                                              |
    |  +->+ DocValues +<-+                                           |
    |  |  +------...--+  |                                           |
    |==|=================|===========================================+- Inverted Text
    ++~+~~~~~~~~~+~~~~~~~+~~+~~~~~~~~+-----------------------...--+  |  Index Section
    || DV START  |  DV END  | LENGTH | VELLUM DATA: TERM -> OFFSET|  |
    ++~~~~~~~~~~~+~~~~~~~~~~+~~~~~~~~+-----------------------...--+  |
    +================================================================+

DocValues is chunked Snappy-compressed values for each document and field.

    [~~~~~~~~~~~~~~~|~~~~~~|~~~~~~~~~|-...-|~~~~~~|~~~~~~~~~|--------------------...-]
    [ Doc# in Chunk | Doc1 | Offset1 | ... | DocN | OffsetN | SNAPPY COMPRESSED DATA ]
    [~~~~~~~~~~~~~~~|~~~~~~|~~~~~~~~~|-...-|~~~~~~|~~~~~~~~~|--------------------...-]

Last 16 bytes are description of chunks.

    |~~~~~~~~~~~~...~|----------------|----------------|
    |   Chunk Sizes  | Chunk Size Arr |         Chunk# |
    |~~~~~~~~~~~~...~|----------------|----------------|
