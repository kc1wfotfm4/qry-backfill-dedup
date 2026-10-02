package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/kc1wfotfm4/qry-backfill-dedup/internal/store"
)

// NewRouter wires the public HTTP surface described in README.md; every entry keeps the single
// top-level error object the service contract publishes.
func NewRouter(st *store.Store) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	router.GET("/healthz", func(c *gin.Context) {
		if err := st.Ping(); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": gin.H{"code": "storage_unavailable", "message": "database is not available"}})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok", "database": "ok"})
	})

	router.POST("/api/v1/backfills", createBatchHandler(st))
	router.GET("/api/v1/backfills/:id", getBatchHandler(st))
	router.GET("/api/v1/datasets/:dataset_id/coverage", getCoverageHandler(st))

	router.NoRoute(func(c *gin.Context) {
		c.JSON(http.StatusNotFound, gin.H{"error": gin.H{"code": "route_not_found", "message": "no route matches this path"}})
	})
	return router
}

// writeError emits the single top-level error object every failure response shares.
func writeError(c *gin.Context, status int, code, message string) {
	c.JSON(status, gin.H{"error": gin.H{"code": code, "message": message}})
}
