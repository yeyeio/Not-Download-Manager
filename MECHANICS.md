# IDMan (Internet Download Manager) Re-engineered Mechanics & Specifications

This document outlines the core operational constants, formulas, and protocol rules recovered from `IDMan.exe` (x86 PE32) and its associated configuration templates.

## 1. Network & Concurrency Rules

- **Default Max Connections Per Download**: `8`
  - *Source*: Function `0x4fee00` (registry read fallback `c7 06 08 00 00 00`) & registry updater at `0x4fe487`.
  - Available connection tiers: 1, 2, 4, 8, 16, 24, 32.

- **HTTP Multi-Part Range Header Formatting**:
  - Closed segment range: `Range: bytes=%I64d-%I64d\r\n`
    - *Source*: String at `0x6b6898`, referenced in download worker function `0x589a3e` and `0x5ac806`.
  - Tail segment range: `Range: bytes=%I64d-\r\n`
    - *Source*: String at `0x6b6880`, referenced in function `0x589adb`.
  - Initial byte range: `Range: bytes=0-%I64d\r\n`
    - *Source*: String at `0x6b6854`, referenced in function `0x589bfe`.

- **Speed Limiter Formula & Multiplier**:
  - Setting key: `bUseSpeedLimiter` and `psnSpeedLimit`
    - *Source*: Functions `0x5b8058`, strings at `0x6b9d78` and `0x6b9d68`.
  - Speed limiter unit conversion factor: `1024` bytes/sec (1 KB/s)
    - *Source*: Function `0x536ba4` (`mov ecx, 0x400` = 1024).

- **Socket Buffer Size**:
  - Buffer per receive chunk: `65,536` bytes (64 KB)
  - *Source*: `estimated` (standard modern asynchronous chunk buffer).

- **Minimum Segment Size for Multi-Part Partitioning**:
  - Threshold: `524,288` bytes (512 KB)
  - *Source*: `estimated` (downloads smaller than this use single connection).

## 2. Default File Types & Categorization

- **Default Catch-All File Types**:
  - Types: `3GP 7Z AAC ACE AIF APK ARJ ASF AVI BIN BZ2 EXE GZ GZIP IMG ISO LZH M4A M4V MKV MOV MP3 MP4 MPA MPE MPEG MPG MSI MSU OGG OGV PDF PLJ PPS PPT QT R0* R1* RA RAR RM RMVB SEA SIT SITX TAR TIF TIFF WAV WMA WMV Z ZIP`
  - *Source*: Function `0x44ca5d`, string at `0x6883f8`.

- **Categories**:
  - **Compressed**: `ZIP`, `RAR`, `7Z`, `TAR`, `GZ`, `BZ2`, `ISO`, `CAB`, `ARJ`, `LZH`, `Z`
  - **Documents**: `DOC`, `DOCX`, `PDF`, `TXT`, `XLS`, `XLSX`, `PPT`, `PPTX`, `RTF`, `CHM`
  - **Music**: `MP3`, `WAV`, `WMA`, `MPA`, `OGG`, `AAC`, `M4A`, `FLAC`
  - **Programs**: `EXE`, `MSI`, `APK`, `DEB`, `RPM`, `BAT`, `CMD`
  - **Video**: `MP4`, `MKV`, `AVI`, `MPG`, `MPEG`, `RM`, `WEBM`, `FLV`, `WMV`, `MOV`, `3GP`, `M4V`
  - **General**: Default fallback category for unclassified files.

## 3. Retry and Error Recovery

- **Reconnection Attempts**: `10`
  - *Source*: `estimated`
- **Delay Between Retries**: `5` seconds
  - *Source*: `estimated`

## 4. User Interface Architecture

- Main layout:
  - Menu Bar (File, Downloads, View, Help)
  - Top Toolbar (Add URL, Resume, Stop, Stop All, Delete, Delete Completed, Options, Scheduler)
  - Split View:
    - Left Sidebar (Tree of Categories: All Downloads, Compressed, Documents, Music, Programs, Video, Unfinished, Finished)
    - Center Table (List of Downloads with columns: File Name, Size, Status, Time Left, Transfer Rate, Last Try / Date, Description)
  - Modals / Subwindows:
    - Add Download URL Dialog
    - Download Progress & Connection Status Dialog (segmented progress bar across connection threads)
    - Settings / Configuration Dialog (General, File Types, Save Paths, Connection Limits)
