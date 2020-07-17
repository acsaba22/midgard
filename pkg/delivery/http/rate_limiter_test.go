package http

import (
	"github.com/labstack/echo/v4"
	. "gopkg.in/check.v1"
	"net/http"
	"net/http/httptest"
	"time"
)

type RateLimitHandlerSuite struct{}

var _ = Suite(&RateLimitHandlerSuite{})

func (s *RateLimitHandlerSuite) TestRateLimit(c *C) {
	e := echo.New()
	h := func(ctx echo.Context) error {
		return nil
	}
	e.GET("/foo", h, NewRateLimitMiddleware(3, 2))
	server := httptest.NewServer(e)
	defer server.Close()
	for i := 0; i < 2; i++ {
		resp, err := http.Get(server.URL + "/foo")
		c.Assert(err, IsNil)
		c.Assert(resp.StatusCode, Equals, http.StatusOK)
	}

	resp, err := http.Get(server.URL + "/foo")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusTooManyRequests)

	time.Sleep(time.Second)
	resp, err = http.Get(server.URL + "/foo")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
}
