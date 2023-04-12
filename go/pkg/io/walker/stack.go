package walker

type stack struct {
	stack []any
}

func (s *stack) push(items ...any) {
	s.stack = append(s.stack, items...)
}

func (s *stack) addLast(item any) {
	s.stack = append([]any{item}, s.stack...)
}

func (s *stack) addPenultimate(item any) {
	if len(s.stack) == 0 {
		s.stack = append(s.stack, item)
		return
	}
	s.stack = append([]any{s.stack[0], item}, s.stack[1:]...)
}

func (s *stack) pop() any {
	p := s.peek()
	if p == nil {
		return nil
	}
	s.stack = s.stack[:len(s.stack)-1]
	return p
}

func (s *stack) peek() any {
	i := len(s.stack) - 1
	if i < 0 {
		return nil
	}
	p := s.stack[i]
	return p
}

func (s *stack) len() int {
	return len(s.stack)
}
