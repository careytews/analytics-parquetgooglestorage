package main

// Parquet file writer.

import (
	"io"	
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetWriter"
	"github.com/xitongsys/parquet-go/parquet"
)

type Writer struct {
	f  ParquetFile.ParquetFile
	pw *ParquetWriter.ParquetWriter
}

func NewWriter(writer io.Writer) (*Writer, error) {

	pf := ParquetFile.NewWriterFile(writer)

	// 4 writer goroutines.
	pw, err := ParquetWriter.NewParquetWriter(pf, new(FlatEvent), 4)
	if err != nil {
		pf.Close()
		return nil, err
	}
	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	w := &Writer{f: pf, pw: pw}

	return w, nil

}

func NewFileWriter(path string) (*Writer, error) {

	f, err := ParquetFile.NewLocalFileWriter(path)
	if err != nil {
		return nil, err
	}

	// 4 writer goroutines.
	pw, err := ParquetWriter.NewParquetWriter(f, new(FlatEvent), 4)
	if err != nil {
		f.Close()
		return nil, err
	}
	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	w := &Writer{f: f, pw: pw}

	return w, nil

}

func (w *Writer) Close() error {
	err := w.pw.WriteStop()
	if err != nil {
		w.f.Close()
		return err
	}
	w.f.Close()
	return nil
}

func (w *Writer) Write(d interface{}) error {

	err := w.pw.Write(d)
	if err != nil {
		return err
	}
	return nil

}
