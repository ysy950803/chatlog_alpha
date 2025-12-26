package http

import (
	"embed"
	"encoding/csv"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/xuri/excelize/v2"

	"github.com/sjzar/chatlog/internal/errors"
	"github.com/sjzar/chatlog/pkg/util"
	"github.com/sjzar/chatlog/pkg/util/dat2img"
	"github.com/sjzar/chatlog/pkg/util/silk"
)

// EFS holds embedded file system data for static assets.
//
//go:embed static
var EFS embed.FS

func (s *Service) initRouter() {
	s.initBaseRouter()
	s.initMediaRouter()
	s.initAPIRouter()
	s.initMCPRouter()
}

func (s *Service) initBaseRouter() {
	staticDir, _ := fs.Sub(EFS, "static")

	s.router.StaticFS("/static", http.FS(staticDir))
	s.router.StaticFileFS("/favicon.ico", "./favicon.ico", http.FS(staticDir))
	s.router.StaticFileFS("/", "./index.htm", http.FS(staticDir))

	s.router.GET("/health", func(ctx *gin.Context) {
		ctx.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	s.router.NoRoute(s.NoRoute)
}

func (s *Service) initMediaRouter() {
	s.router.GET("/image/*key", func(c *gin.Context) { s.handleMedia(c, "image") })
	s.router.GET("/video/*key", func(c *gin.Context) { s.handleMedia(c, "video") })
	s.router.GET("/file/*key", func(c *gin.Context) { s.handleMedia(c, "file") })
	s.router.GET("/voice/*key", func(c *gin.Context) { s.handleMedia(c, "voice") })
	s.router.GET("/data/*path", s.handleMediaData)
}

func (s *Service) initAPIRouter() {
	api := s.router.Group("/api/v1", s.checkDBStateMiddleware())
	{
		api.GET("/chatlog", s.handleChatlog)
		api.GET("/contact", s.handleContacts)
		api.GET("/chatroom", s.handleChatRooms)
		api.GET("/session", s.handleSessions)
		api.GET("/db", s.handleGetDBs)
		api.GET("/db/tables", s.handleGetDBTables)
		api.GET("/db/data", s.handleGetDBTableData)
		api.GET("/db/query", s.handleExecuteSQL)
		api.POST("/cache/clear", s.handleClearCache)
	}
}

func (s *Service) initMCPRouter() {
	s.router.Any("/mcp", func(c *gin.Context) {
		s.mcpStreamableServer.ServeHTTP(c.Writer, c.Request)
	})
	s.router.Any("/sse", func(c *gin.Context) {
		s.mcpSSEServer.ServeHTTP(c.Writer, c.Request)
	})
	s.router.Any("/message", func(c *gin.Context) {
		s.mcpSSEServer.ServeHTTP(c.Writer, c.Request)
	})
}

// NoRoute handles 404 Not Found errors. If the request URL starts with "/api"
// or "/static", it responds with a JSON error. Otherwise, it redirects to the root path.
func (s *Service) NoRoute(c *gin.Context) {
	path := c.Request.URL.Path
	switch {
	case strings.HasPrefix(path, "/api"), strings.HasPrefix(path, "/static"):
		c.JSON(http.StatusNotFound, gin.H{"error": "Not found"})
	default:
		c.Header("Cache-Control", "no-cache, no-store, max-age=0, must-revalidate, value")
		c.Redirect(http.StatusFound, "/")
	}
}

func (s *Service) handleChatlog(c *gin.Context) {

	q := struct {
		Time    string `form:"time"`
		Talker  string `form:"talker"`
		Sender  string `form:"sender"`
		Keyword string `form:"keyword"`
		Limit   int    `form:"limit"`
		Offset  int    `form:"offset"`
		Format  string `form:"format"`
	}{}

	if err := c.BindQuery(&q); err != nil {
		errors.Err(c, err)
		return
	}

	var err error
	start, end, ok := util.TimeRangeOf(q.Time)
	if !ok {
		errors.Err(c, errors.InvalidArg("time"))
	}
	if q.Limit < 0 {
		q.Limit = 0
	}

	if q.Offset < 0 {
		q.Offset = 0
	}

	messages, err := s.db.GetMessages(start, end, q.Talker, q.Sender, q.Keyword, q.Limit, q.Offset)
	if err != nil {
		errors.Err(c, err)
		return
	}

	switch strings.ToLower(q.Format) {
	case "csv":
		c.Writer.Header().Set("Content-Type", "text/csv; charset=utf-8")
		c.Writer.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s_%s_%s.csv", q.Talker, start.Format("2006-01-02"), end.Format("2006-01-02")))
		c.Writer.Header().Set("Cache-Control", "no-cache")
		c.Writer.Header().Set("Connection", "keep-alive")
		c.Writer.Flush()

		csvWriter := csv.NewWriter(c.Writer)
		csvWriter.Write([]string{"MessageID", "Time", "SenderName", "Sender", "TalkerName", "Talker", "Content"})
		for _, m := range messages {
			csvWriter.Write(m.CSV(c.Request.Host))
		}
		csvWriter.Flush()
	case "xlsx", "excel":
		f := excelize.NewFile()
		defer func() {
			if err := f.Close(); err != nil {
				log.Error().Err(err).Msg("Failed to close excel file")
			}
		}()
		// Create a new sheet.
		index, err := f.NewSheet("Sheet1")
		if err != nil {
			errors.Err(c, err)
			return
		}
		// Set value of a cell.
		headers := []string{"MessageID", "Time", "SenderName", "Sender", "TalkerName", "Talker", "Content"}
		for i, header := range headers {
			cell, _ := excelize.CoordinatesToCellName(i+1, 1)
			f.SetCellValue("Sheet1", cell, header)
		}
		for i, m := range messages {
			row := m.CSV(c.Request.Host)
			for j, val := range row {
				cell, _ := excelize.CoordinatesToCellName(j+1, i+2)
				f.SetCellValue("Sheet1", cell, val)
			}
		}
		f.SetActiveSheet(index)
		// Set headers
		c.Writer.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
		c.Writer.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s_%s_%s.xlsx", q.Talker, start.Format("2006-01-02"), end.Format("2006-01-02")))
		if err := f.Write(c.Writer); err != nil {
			errors.Err(c, err)
			return
		}
	case "json":
		// json
		c.JSON(http.StatusOK, messages)
	default:
		// plain text
		c.Writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
		c.Writer.Header().Set("Cache-Control", "no-cache")
		c.Writer.Header().Set("Connection", "keep-alive")
		c.Writer.Flush()

		for _, m := range messages {
			c.Writer.WriteString(m.PlainText(strings.Contains(q.Talker, ","), util.PerfectTimeFormat(start, end), c.Request.Host))
			c.Writer.WriteString("\n")
			c.Writer.Flush()
		}
	}
}

func (s *Service) handleContacts(c *gin.Context) {

	q := struct {
		Keyword string `form:"keyword"`
		Limit   int    `form:"limit"`
		Offset  int    `form:"offset"`
		Format  string `form:"format"`
	}{}

	if err := c.BindQuery(&q); err != nil {
		errors.Err(c, err)
		return
	}

	list, err := s.db.GetContacts(q.Keyword, q.Limit, q.Offset)
	if err != nil {
		errors.Err(c, err)
		return
	}

	format := strings.ToLower(q.Format)
	switch format {
	case "json":
		// json
		c.JSON(http.StatusOK, list)
	case "xlsx", "excel":
		f := excelize.NewFile()
		defer func() {
			if err := f.Close(); err != nil {
				log.Error().Err(err).Msg("Failed to close excel file")
			}
		}()
		index, err := f.NewSheet("Contacts")
		if err != nil {
			errors.Err(c, err)
			return
		}
		headers := []string{"UserName", "Alias", "Remark", "NickName"}
		for i, header := range headers {
			cell, _ := excelize.CoordinatesToCellName(i+1, 1)
			f.SetCellValue("Contacts", cell, header)
		}
		for i, contact := range list.Items {
			row := []interface{}{contact.UserName, contact.Alias, contact.Remark, contact.NickName}
			for j, val := range row {
				cell, _ := excelize.CoordinatesToCellName(j+1, i+2)
				f.SetCellValue("Contacts", cell, val)
			}
		}
		f.SetActiveSheet(index)
		c.Writer.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
		c.Writer.Header().Set("Content-Disposition", "attachment; filename=contacts.xlsx")
		if err := f.Write(c.Writer); err != nil {
			errors.Err(c, err)
			return
		}
	default:
		// csv
		if format == "csv" {
			// 浏览器访问时，会下载文件
			c.Writer.Header().Set("Content-Type", "text/csv; charset=utf-8")
		} else {
			c.Writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
		}
		c.Writer.Header().Set("Cache-Control", "no-cache")
		c.Writer.Header().Set("Connection", "keep-alive")
		c.Writer.Flush()

		c.Writer.WriteString("UserName,Alias,Remark,NickName\n")
		for _, contact := range list.Items {
			c.Writer.WriteString(fmt.Sprintf("%s,%s,%s,%s\n", contact.UserName, contact.Alias, contact.Remark, contact.NickName))
		}
		c.Writer.Flush()
	}
}

func (s *Service) handleChatRooms(c *gin.Context) {

	q := struct {
		Keyword string `form:"keyword"`
		Limit   int    `form:"limit"`
		Offset  int    `form:"offset"`
		Format  string `form:"format"`
	}{}

	if err := c.BindQuery(&q); err != nil {
		errors.Err(c, err)
		return
	}

	list, err := s.db.GetChatRooms(q.Keyword, q.Limit, q.Offset)
	if err != nil {
		errors.Err(c, err)
		return
	}
	format := strings.ToLower(q.Format)
	switch format {
	case "json":
		// json
		c.JSON(http.StatusOK, list)
	case "xlsx", "excel":
		f := excelize.NewFile()
		defer func() {
			if err := f.Close(); err != nil {
				log.Error().Err(err).Msg("Failed to close excel file")
			}
		}()
		index, err := f.NewSheet("ChatRooms")
		if err != nil {
			errors.Err(c, err)
			return
		}
		headers := []string{"Name", "Remark", "NickName", "Owner", "UserCount"}
		for i, header := range headers {
			cell, _ := excelize.CoordinatesToCellName(i+1, 1)
			f.SetCellValue("ChatRooms", cell, header)
		}
		for i, chatRoom := range list.Items {
			row := []interface{}{chatRoom.Name, chatRoom.Remark, chatRoom.NickName, chatRoom.Owner, len(chatRoom.Users)}
			for j, val := range row {
				cell, _ := excelize.CoordinatesToCellName(j+1, i+2)
				f.SetCellValue("ChatRooms", cell, val)
			}
		}
		f.SetActiveSheet(index)
		c.Writer.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
		c.Writer.Header().Set("Content-Disposition", "attachment; filename=chatrooms.xlsx")
		if err := f.Write(c.Writer); err != nil {
			errors.Err(c, err)
			return
		}
	default:
		// csv
		if format == "csv" {
			// 浏览器访问时，会下载文件
			c.Writer.Header().Set("Content-Type", "text/csv; charset=utf-8")
		} else {
			c.Writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
		}
		c.Writer.Header().Set("Cache-Control", "no-cache")
		c.Writer.Header().Set("Connection", "keep-alive")
		c.Writer.Flush()

		c.Writer.WriteString("Name,Remark,NickName,Owner,UserCount\n")
		for _, chatRoom := range list.Items {
			c.Writer.WriteString(fmt.Sprintf("%s,%s,%s,%s,%d\n", chatRoom.Name, chatRoom.Remark, chatRoom.NickName, chatRoom.Owner, len(chatRoom.Users)))
		}
		c.Writer.Flush()
	}
}

func (s *Service) handleSessions(c *gin.Context) {

	q := struct {
		Keyword string `form:"keyword"`
		Limit   int    `form:"limit"`
		Offset  int    `form:"offset"`
		Format  string `form:"format"`
	}{}

	if err := c.BindQuery(&q); err != nil {
		errors.Err(c, err)
		return
	}

	sessions, err := s.db.GetSessions(q.Keyword, q.Limit, q.Offset)
	if err != nil {
		errors.Err(c, err)
		return
	}
	format := strings.ToLower(q.Format)
	switch format {
	case "csv":
		c.Writer.Header().Set("Content-Type", "text/csv; charset=utf-8")
		c.Writer.Header().Set("Cache-Control", "no-cache")
		c.Writer.Header().Set("Connection", "keep-alive")
		c.Writer.Flush()

		c.Writer.WriteString("UserName,NOrder,NickName,Content,NTime\n")
		for _, session := range sessions.Items {
			c.Writer.WriteString(fmt.Sprintf("%s,%d,%s,%s,%s\n", session.UserName, session.NOrder, session.NickName, strings.ReplaceAll(session.Content, "\n", "\\n"), session.NTime))
		}
		c.Writer.Flush()
	case "xlsx", "excel":
		f := excelize.NewFile()
		defer func() {
			if err := f.Close(); err != nil {
				log.Error().Err(err).Msg("Failed to close excel file")
			}
		}()
		index, err := f.NewSheet("Sessions")
		if err != nil {
			errors.Err(c, err)
			return
		}
		headers := []string{"UserName", "NOrder", "NickName", "Content", "NTime"}
		for i, header := range headers {
			cell, _ := excelize.CoordinatesToCellName(i+1, 1)
			f.SetCellValue("Sessions", cell, header)
		}
		for i, session := range sessions.Items {
			row := []interface{}{session.UserName, session.NOrder, session.NickName, session.Content, session.NTime}
			for j, val := range row {
				cell, _ := excelize.CoordinatesToCellName(j+1, i+2)
				f.SetCellValue("Sessions", cell, val)
			}
		}
		f.SetActiveSheet(index)
		c.Writer.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
		c.Writer.Header().Set("Content-Disposition", "attachment; filename=sessions.xlsx")
		if err := f.Write(c.Writer); err != nil {
			errors.Err(c, err)
			return
		}
	case "json":
		// json
		c.JSON(http.StatusOK, sessions)
	default:
		c.Writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
		c.Writer.Header().Set("Cache-Control", "no-cache")
		c.Writer.Header().Set("Connection", "keep-alive")
		c.Writer.Flush()
		for _, session := range sessions.Items {
			c.Writer.WriteString(session.PlainText(120))
			c.Writer.WriteString("\n")
		}
		c.Writer.Flush()
	}
}

func (s *Service) handleMedia(c *gin.Context, _type string) {
	key := strings.TrimPrefix(c.Param("key"), "/")
	if key == "" {
		errors.Err(c, errors.InvalidArg(key))
		return
	}

	keys := util.Str2List(key, ",")
	if len(keys) == 0 {
		errors.Err(c, errors.InvalidArg(key))
		return
	}

	var _err error
	for _, k := range keys {
		if strings.Contains(k, "/") {
			if absolutePath, err := s.findPath(_type, k); err == nil {
				c.Redirect(http.StatusFound, "/data/"+absolutePath)
				return
			}
		}
		media, err := s.db.GetMedia(_type, k)
		if err != nil {
			_err = err
			continue
		}
		if c.Query("info") != "" {
			c.JSON(http.StatusOK, media)
			return
		}
		switch media.Type {
		case "voice":
			s.HandleVoice(c, media.Data)
			return
		case "image":
			// If it's not a .dat file, redirect to the data handler as before.
			if !strings.HasSuffix(strings.ToLower(media.Path), ".dat") {
				c.Redirect(http.StatusFound, "/data/"+media.Path)
				return
			}

			// It is a .dat file. Decrypt, save, and redirect to the new file.
			absolutePath := filepath.Join(s.conf.GetDataDir(), media.Path)

			// Build the potential output path to check if it exists
			var newRelativePath string
			outputPath := strings.TrimSuffix(absolutePath, filepath.Ext(absolutePath))
			relativePathBase := strings.TrimSuffix(media.Path, filepath.Ext(media.Path))

			// Check if a converted file already exists
			for _, ext := range []string{".jpg", ".png", ".gif", ".jpeg", ".bmp", ".mp4"} {
				if _, err := os.Stat(outputPath + ext); err == nil {
					newRelativePath = relativePathBase + ext
					break
				}
			}

			// If a converted file is found, redirect to it immediately
			if newRelativePath != "" {
				c.Redirect(http.StatusFound, "/data/"+newRelativePath)
				return
			}

			// If not found, decrypt and save it
			b, err := os.ReadFile(absolutePath)
			if err != nil {
				errors.Err(c, err)
				return
			}

			out, ext, err := dat2img.Dat2Image(b)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{
					"error":  "Failed to parse .dat file",
					"reason": err.Error(),
					"path":   absolutePath,
				})
				return
			}

			// Save the decrypted file. s.saveDecryptedFile handles the existence check.
			s.saveDecryptedFile(absolutePath, out, ext)

			// Build the new relative path and redirect
			newRelativePath = relativePathBase + "." + ext
			c.Redirect(http.StatusFound, "/data/"+newRelativePath)
			return

		default:
			// For other types, keep the old redirect logic
			c.Redirect(http.StatusFound, "/data/"+media.Path)
			return
		}
	}

	if _err != nil {
		errors.Err(c, _err)
		return
	}
}

func (s *Service) findPath(_type string, key string) (string, error) {
	absolutePath := filepath.Join(s.conf.GetDataDir(), key)
	if _, err := os.Stat(absolutePath); err == nil {
		return key, nil
	}
	switch _type {
	case "image":
		for _, suffix := range []string{"_h.dat", ".dat", "_t.dat"} {
			if _, err := os.Stat(absolutePath + suffix); err == nil {
				return key + suffix, nil
			}
		}
	case "video":
		for _, suffix := range []string{".mp4", "_thumb.jpg"} {
			if _, err := os.Stat(absolutePath + suffix); err == nil {
				return key + suffix, nil
			}
		}
	}
	return "", errors.ErrMediaNotFound
}

func (s *Service) handleMediaData(c *gin.Context) {
	relativePath := filepath.Clean(c.Param("path"))

	absolutePath := filepath.Join(s.conf.GetDataDir(), relativePath)

	if _, err := os.Stat(absolutePath); os.IsNotExist(err) {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "File not found",
		})
		return
	}

	ext := strings.ToLower(filepath.Ext(absolutePath))
	switch {
	case ext == ".dat":
		s.HandleDatFile(c, absolutePath)
	default:
		// 直接返回文件
		c.File(absolutePath)
	}

}

func (s *Service) HandleDatFile(c *gin.Context, path string) {

	b, err := os.ReadFile(path)
	if err != nil {
		errors.Err(c, err)
		return
	}
	out, ext, err := dat2img.Dat2Image(b)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":  "Failed to parse .dat file",
			"reason": err.Error(),
			"path":   path,
		})
		return
	}

	// Save decrypted file to local disk
	if s.conf.GetSaveDecryptedMedia() {
		s.saveDecryptedFile(path, out, ext)
	}

	switch ext {
	case "jpg", "jpeg":
		c.Data(http.StatusOK, "image/jpeg", out)
	case "png":
		c.Data(http.StatusOK, "image/png", out)
	case "gif":
		c.Data(http.StatusOK, "image/gif", out)
	case "bmp":
		c.Data(http.StatusOK, "image/bmp", out)
	case "mp4":
		c.Data(http.StatusOK, "video/mp4", out)
	default:
		c.Data(http.StatusOK, "image/jpg", out)
		// c.File(path)
	}
}

func (s *Service) HandleVoice(c *gin.Context, data []byte) {
	out, err := silk.Silk2MP3(data)
	if err != nil {
		c.Data(http.StatusOK, "audio/silk", data)
		return
	}
	c.Data(http.StatusOK, "audio/mp3", out)
}

// saveDecryptedFile saves the decrypted media file to local disk
func (s *Service) saveDecryptedFile(datPath string, data []byte, ext string) {
	// Generate target file path: replace .dat with actual extension
	outputPath := strings.TrimSuffix(datPath, filepath.Ext(datPath)) + "." + ext

	// Check if file already exists to avoid duplicate writes
	if _, err := os.Stat(outputPath); err == nil {
		return
	}

	// Write file
	if err := os.WriteFile(outputPath, data, 0644); err != nil {
		log.Error().
			Err(err).
			Str("dat_path", datPath).
			Str("output_path", outputPath).
			Msg("Failed to save decrypted file")
		return
	}

	log.Debug().
		Str("dat_path", datPath).
		Str("output_path", outputPath).
		Str("format", ext).
		Int("size", len(data)).
		Msg("Decrypted file saved successfully")
}

func (s *Service) handleClearCache(c *gin.Context) {
	dataDir := s.conf.GetDataDir()
	if dataDir == "" {
		errors.Err(c, fmt.Errorf("data directory not configured"))
		return
	}

	deletedCount := 0

	err := filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil // Skip directories
		}

		ext := strings.ToLower(filepath.Ext(path))
		// List of generated extensions
		generatedExts := map[string]struct{}{
			".jpg": {}, ".jpeg": {}, ".png": {}, ".gif": {}, ".bmp": {}, ".mp4": {},
		}

		if _, isGenerated := generatedExts[ext]; isGenerated {
			baseName := strings.TrimSuffix(path, ext)
			// Check for corresponding .dat file. WeChat can use various suffixes.
			datSuffixes := []string{".dat", "_h.dat", "_t.dat"}
			for _, datSuffix := range datSuffixes {
				datPath := baseName + datSuffix
				if _, statErr := os.Stat(datPath); statErr == nil {
					// Found a corresponding .dat file, so this is a cached file.
					if removeErr := os.Remove(path); removeErr == nil {
						deletedCount++
					} else {
						log.Warn().Err(removeErr).Str("path", path).Msg("Failed to remove cached file")
					}
					// Once we find a .dat pair and delete, no need to check other suffixes
					return nil
				}
			}
		}
		return nil
	})

	if err != nil {
		errors.Err(c, fmt.Errorf("failed to walk data directory: %w", err))
		return
	}

	log.Info().Int("count", deletedCount).Msg("Cleared decrypted file cache")
	c.JSON(http.StatusOK, gin.H{
		"message":      "Cache cleared successfully",
		"deletedCount": deletedCount,
	})
}

func (s *Service) handleGetDBs(c *gin.Context) {
	dbs, err := s.db.GetDecryptedDBs()
	if err != nil {
		errors.Err(c, err)
		return
	}
	c.JSON(http.StatusOK, dbs)
}

func (s *Service) handleGetDBTables(c *gin.Context) {
	group := c.Query("group")
	file := c.Query("file")

	if group == "" || file == "" {
		errors.Err(c, errors.InvalidArg("group or file"))
		return
	}

	tables, err := s.db.GetTables(group, file)
	if err != nil {
		errors.Err(c, err)
		return
	}
	c.JSON(http.StatusOK, tables)
}

func (s *Service) handleGetDBTableData(c *gin.Context) {
	group := c.Query("group")
	file := c.Query("file")
	table := c.Query("table")
	keyword := c.Query("keyword")
	limitStr := c.DefaultQuery("limit", "20")
	offsetStr := c.DefaultQuery("offset", "0")
	format := strings.ToLower(c.Query("format"))

	if group == "" || file == "" || table == "" {
		errors.Err(c, errors.InvalidArg("group, file or table"))
		return
	}

	limit := 20
	offset := 0
	fmt.Sscanf(limitStr, "%d", &limit)
	fmt.Sscanf(offsetStr, "%d", &offset)

	// If exporting, fetch all matching rows (ignore pagination if user wants all? or respect pagination?)
	// Usually export means "export all matching".
	if format == "csv" || format == "xlsx" || format == "excel" {
		limit = -1 // No limit
		offset = 0
	}

	data, err := s.db.GetTableData(group, file, table, limit, offset, keyword)
	if err != nil {
		errors.Err(c, err)
		return
	}

	if format == "csv" || format == "xlsx" || format == "excel" {
		s.exportData(c, data, format, table)
		return
	}
	
	c.JSON(http.StatusOK, data)
}

func (s *Service) handleExecuteSQL(c *gin.Context) {
	group := c.Query("group")
	file := c.Query("file")
	query := c.Query("sql")
	format := strings.ToLower(c.Query("format"))

	if group == "" || file == "" || query == "" {
		errors.Err(c, errors.InvalidArg("group, file or sql"))
		return
	}

	data, err := s.db.ExecuteSQL(group, file, query)
	if err != nil {
		errors.Err(c, err)
		return
	}

	if format == "csv" || format == "xlsx" || format == "excel" {
		s.exportData(c, data, format, "query_result")
		return
	}

	c.JSON(http.StatusOK, data)
}

func (s *Service) exportData(c *gin.Context, data []map[string]interface{}, format string, filename string) {
	if len(data) == 0 {
		c.String(http.StatusOK, "")
		return
	}

	// Extract headers
	var headers []string
	for k := range data[0] {
		headers = append(headers, k)
	}
	// Sort headers for consistency
	// sort.Strings(headers) // We need sort package

	if format == "csv" {
		c.Writer.Header().Set("Content-Type", "text/csv; charset=utf-8")
		c.Writer.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s.csv", filename))
		c.Writer.Header().Set("Cache-Control", "no-cache")
		c.Writer.Flush()

		w := csv.NewWriter(c.Writer)
		w.Write(headers)
		for _, row := range data {
			var record []string
			for _, h := range headers {
				val := row[h]
				if val == nil {
					record = append(record, "")
				} else {
					record = append(record, fmt.Sprintf("%v", val))
				}
			}
			w.Write(record)
		}
		w.Flush()
	} else {
		// Excel
		f := excelize.NewFile()
		defer func() {
			if err := f.Close(); err != nil {
				log.Error().Err(err).Msg("Failed to close excel file")
			}
		}()
		
		sheet := "Sheet1"
		index, _ := f.NewSheet(sheet)
		
		// Write headers
		for i, h := range headers {
			cell, _ := excelize.CoordinatesToCellName(i+1, 1)
			f.SetCellValue(sheet, cell, h)
		}
		
		// Write data
		for r, row := range data {
			for cIdx, h := range headers {
				val := row[h]
				cell, _ := excelize.CoordinatesToCellName(cIdx+1, r+2)
				f.SetCellValue(sheet, cell, val)
			}
		}
		
		f.SetActiveSheet(index)
		c.Writer.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
		c.Writer.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s.xlsx", filename))
		if err := f.Write(c.Writer); err != nil {
			log.Error().Err(err).Msg("Failed to write excel file")
		}
	}
}

