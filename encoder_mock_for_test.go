package ftdc

type MockEncoder struct {
	Inputs        []int64
	AddError      error
	ResolveOutput []byte
	ResolveError  error
	ResetCalled   bool
	ReportedSize  int
}

func (e *MockEncoder) Add(in int64) error {
	if e.AddError != nil {
		return e.AddError
	}
	e.Inputs = append(e.Inputs, in)
	return nil
}
func (e *MockEncoder) Resolve() ([]byte, error) { return e.ResolveOutput, e.ResolveError }
func (e *MockEncoder) Reset()                   { e.ResetCalled = true }
func (e *MockEncoder) Size() int                { return e.ReportedSize }
