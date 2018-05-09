package yfs

type Op interface {
	OpName() string
}

type OpCreatBlock struct {
	Id []byte
}

func (o *OpCreatBlock) OpName() string {
	return "create-block"
}

type OpRefBlock struct {
	Id []byte
}

func (o *OpRefBlock) OpName() string {
	return "ref-block"
}

type OpUpdateFile struct {
	Path string
}

func (o *OpUpdateFile) OpName() string {
	return "update-file"
}
