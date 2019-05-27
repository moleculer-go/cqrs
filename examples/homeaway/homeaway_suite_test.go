package homeaway

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestHomeaway(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Homeaway Suite")
}
