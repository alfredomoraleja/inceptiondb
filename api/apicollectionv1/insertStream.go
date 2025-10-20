package apicollectionv1

import (
	"context"
	"io"
	"net/http"
	"net/http/httputil"
)

// how to try with curl:
// start with tls: HTTPSENABLED=TRUE HTTPSSELFSIGNED=TRUE make run
// curl -v -X POST -T. -k https://localhost:8080/v1/collections/prueba:insert
// type one document and press enter
func insertStream(ctx context.Context, w http.ResponseWriter, r *http.Request) error {

	return nil
}

func FullDuplex(w http.ResponseWriter, f func(w io.Writer)) {

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijacking not supported", 500)
		return
	}

	conn, bufrw, err := hj.Hijack()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer conn.Close()

	_, err = bufrw.WriteString("HTTP/1.1 202 " + http.StatusText(http.StatusAccepted) + "\r\n")
	w.Header().Write(bufrw)
	_, err = bufrw.WriteString("Transfer-Encoding: chunked\r\n")
	_, err = bufrw.WriteString("\r\n")

	chunkedw := httputil.NewChunkedWriter(bufrw)

	f(chunkedw)

	chunkedw.Close()
	_, err = bufrw.WriteString("\r\n")

	bufrw.Flush()
}
