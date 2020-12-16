// Code generated by easyjson for marshaling/unmarshaling. DO NOT EDIT.

package event

import (
	json "encoding/json"
	easyjson "github.com/mailru/easyjson"
	jlexer "github.com/mailru/easyjson/jlexer"
	jwriter "github.com/mailru/easyjson/jwriter"
)

// suppress unused package warning
var (
	_ *json.RawMessage
	_ *jlexer.Lexer
	_ *jwriter.Writer
	_ easyjson.Marshaler
)

func easyjsonF642ad3eDecodeGithubComTeamlintPgFlowEvent(in *jlexer.Lexer, out *Event) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeFieldName(false)
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "id":
			out.ID = string(in.String())
		case "schema":
			out.Schema = string(in.String())
		case "table":
			out.Table = string(in.String())
		case "action":
			out.Action = string(in.String())
		case "data":
			if in.IsNull() {
				in.Skip()
			} else {
				in.Delim('{')
				out.Data = make(map[string]interface{})
				for !in.IsDelim('}') {
					key := string(in.String())
					in.WantColon()
					var v1 interface{}
					if m, ok := v1.(easyjson.Unmarshaler); ok {
						m.UnmarshalEasyJSON(in)
					} else if m, ok := v1.(json.Unmarshaler); ok {
						_ = m.UnmarshalJSON(in.Raw())
					} else {
						v1 = in.Interface()
					}
					(out.Data)[key] = v1
					in.WantComma()
				}
				in.Delim('}')
			}
		case "commit_time":
			if data := in.Raw(); in.Ok() {
				in.AddError((out.CommitTime).UnmarshalJSON(data))
			}
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}
func easyjsonF642ad3eEncodeGithubComTeamlintPgFlowEvent(out *jwriter.Writer, in Event) {
	out.RawByte('{')
	first := true
	_ = first
	{
		const prefix string = ",\"id\":"
		out.RawString(prefix[1:])
		out.String(string(in.ID))
	}
	{
		const prefix string = ",\"schema\":"
		out.RawString(prefix)
		out.String(string(in.Schema))
	}
	{
		const prefix string = ",\"table\":"
		out.RawString(prefix)
		out.String(string(in.Table))
	}
	{
		const prefix string = ",\"action\":"
		out.RawString(prefix)
		out.String(string(in.Action))
	}
	{
		const prefix string = ",\"data\":"
		out.RawString(prefix)
		if in.Data == nil && (out.Flags&jwriter.NilMapAsEmpty) == 0 {
			out.RawString(`null`)
		} else {
			out.RawByte('{')
			v2First := true
			for v2Name, v2Value := range in.Data {
				if v2First {
					v2First = false
				} else {
					out.RawByte(',')
				}
				out.String(string(v2Name))
				out.RawByte(':')
				if m, ok := v2Value.(easyjson.Marshaler); ok {
					m.MarshalEasyJSON(out)
				} else if m, ok := v2Value.(json.Marshaler); ok {
					out.Raw(m.MarshalJSON())
				} else {
					out.Raw(json.Marshal(v2Value))
				}
			}
			out.RawByte('}')
		}
	}
	{
		const prefix string = ",\"commit_time\":"
		out.RawString(prefix)
		out.Raw((in.CommitTime).MarshalJSON())
	}
	out.RawByte('}')
}

// MarshalJSON supports json.Marshaler interface
func (v Event) MarshalJSON() ([]byte, error) {
	w := jwriter.Writer{}
	easyjsonF642ad3eEncodeGithubComTeamlintPgFlowEvent(&w, v)
	return w.Buffer.BuildBytes(), w.Error
}

// MarshalEasyJSON supports easyjson.Marshaler interface
func (v Event) MarshalEasyJSON(w *jwriter.Writer) {
	easyjsonF642ad3eEncodeGithubComTeamlintPgFlowEvent(w, v)
}

// UnmarshalJSON supports json.Unmarshaler interface
func (v *Event) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	easyjsonF642ad3eDecodeGithubComTeamlintPgFlowEvent(&r, v)
	return r.Error()
}

// UnmarshalEasyJSON supports easyjson.Unmarshaler interface
func (v *Event) UnmarshalEasyJSON(l *jlexer.Lexer) {
	easyjsonF642ad3eDecodeGithubComTeamlintPgFlowEvent(l, v)
}
