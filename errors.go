package pulsar

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

func newError(resp *http.Response) error {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("HTTP Status Code: %d, %v", resp.StatusCode, err)
	}
	return fmt.Errorf("HTTP Status Code: %d, %s", resp.StatusCode, string(body))
}
