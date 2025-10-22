package configuration

type Configuration struct {
	HttpAddr          string `usage:"HTTP address"`
	HttpsEnabled      bool   `usage:""`
	HttpsSelfsigned   bool   `usage:""`
	MySQLAddr         string `usage:"MySQL-compatible address"`
	MySQLUser         string `usage:"MySQL user"`
	MySQLPassword     string `usage:"MySQL password"`
	Dir               string `usage:"data directory"`
	Statics           string `usage:"statics directory"`
	Version           bool   `usage:"show version and exit"`
	ShowBanner        bool   `usage:"show big banner"`
	ShowConfig        bool   `usage:"print config"`
	EnableCompression bool   `usage:"enable http compression (gzip)"`
}
