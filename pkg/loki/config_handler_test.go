package loki

import (
	"io"
	"net/http/httptest"
	"testing"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

type diffConfigMock struct {
	MyInt          int          `yaml:"my_int"`
	MyFloat        float64      `yaml:"my_float"`
	MySlice        []string     `yaml:"my_slice"`
	IgnoredField   func() error `yaml:"-"`
	MyNestedStruct struct {
		MyString      string   `yaml:"my_string"`
		MyBool        bool     `yaml:"my_bool"`
		MyEmptyStruct struct{} `yaml:"my_empty_struct"`
	} `yaml:"my_nested_struct"`
}

func newDefaultDiffConfigMock() *diffConfigMock {
	c := &diffConfigMock{
		MyInt:        666,
		MyFloat:      6.66,
		MySlice:      []string{"value1", "value2"},
		IgnoredField: func() error { return nil },
	}
	c.MyNestedStruct.MyString = "string1"
	return c
}

type mockTenantLimits map[string]*validation.Limits

func (tl mockTenantLimits) TenantLimits(userID string) *validation.Limits {
	return tl[userID]
}

func (tl mockTenantLimits) AllByUserID() map[string]*validation.Limits {
	return tl
}

func TestConfigDiffHandler(t *testing.T) {
	for _, tc := range []struct {
		name               string
		expectedStatusCode int
		expectedBody       string
		actualConfig       func() interface{}
	}{
		{
			name:               "no config parameters overridden",
			expectedStatusCode: 200,
			expectedBody:       "{}\n",
		},
		{
			name: "slice changed",
			actualConfig: func() interface{} {
				c := newDefaultDiffConfigMock()
				c.MySlice = append(c.MySlice, "value3")
				return c
			},
			expectedStatusCode: 200,
			expectedBody: "my_slice:\n" +
				"- value1\n" +
				"- value2\n" +
				"- value3\n",
		},
		{
			name: "string in nested struct changed",
			actualConfig: func() interface{} {
				c := newDefaultDiffConfigMock()
				c.MyNestedStruct.MyString = "string2"
				return c
			},
			expectedStatusCode: 200,
			expectedBody: "my_nested_struct:\n" +
				"  my_string: string2\n",
		},
		{
			name: "bool in nested struct changed",
			actualConfig: func() interface{} {
				c := newDefaultDiffConfigMock()
				c.MyNestedStruct.MyBool = true
				return c
			},
			expectedStatusCode: 200,
			expectedBody: "my_nested_struct:\n" +
				"  my_bool: true\n",
		},
		{
			name: "test invalid input",
			actualConfig: func() interface{} {
				c := "x"
				return &c
			},
			expectedStatusCode: 500,
			expectedBody: "yaml: unmarshal errors:\n" +
				"  line 1: cannot unmarshal !!str `x` into map[interface {}]interface {}\n",
		},
	} {
		defaultCfg := newDefaultDiffConfigMock()
		t.Run(tc.name, func(t *testing.T) {

			var actualCfg interface{}
			if tc.actualConfig != nil {
				actualCfg = tc.actualConfig()
			} else {
				actualCfg = newDefaultDiffConfigMock()
			}

			req := httptest.NewRequest("GET", "http://test.com/config?mode=diff", nil)
			w := httptest.NewRecorder()

			h := configHandler(actualCfg, defaultCfg)
			h(w, req)
			resp := w.Result()
			assert.Equal(t, tc.expectedStatusCode, resp.StatusCode)

			body, err := io.ReadAll(resp.Body)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedBody, string(body))
		})
	}

}

func TestTenantLimitsHandlerAllowlist(t *testing.T) {
	limits := &validation.Limits{}
	flagext.DefaultValues(limits)
	tl := mockTenantLimits{"user": limits}
	overrides, err := validation.NewOverrides(validation.Limits{}, tl)
	require.NoError(t, err)

	l := &Loki{
		TenantLimits:          tl,
		Overrides:             overrides,
		TenantLimitsAllowList: nil,
	}

	req := httptest.NewRequest("GET", "http://test.com/config/tenant/v1/limits", nil)
	req.Header.Set("X-Scope-OrgID", "user")
	w := httptest.NewRecorder()
	l.tenantLimitsHandler()(w, req)
	resp := w.Result()
	var gotAll map[string]interface{}
	err = yaml.NewDecoder(resp.Body).Decode(&gotAll)
	require.NoError(t, err)

	expectedAll, err := limitsToAllowedMap(limits, nil)
	require.NoError(t, err)
	assert.Equal(t, expectedAll, gotAll)

	l.TenantLimitsAllowList = []string{"max_label_name_length", "max_label_value_length"}
	req = httptest.NewRequest("GET", "http://test.com/config/tenant/v1/limits", nil)
	req.Header.Set("X-Scope-OrgID", "user")
	w = httptest.NewRecorder()
	l.tenantLimitsHandler()(w, req)
	resp = w.Result()
	var gotFiltered map[string]interface{}
	err = yaml.NewDecoder(resp.Body).Decode(&gotFiltered)
	require.NoError(t, err)

	expectedFiltered, err := limitsToAllowedMap(limits, l.TenantLimitsAllowList)
	require.NoError(t, err)
	assert.Equal(t, expectedFiltered, gotFiltered)
}
