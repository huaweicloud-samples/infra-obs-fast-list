"""对象过滤表达式——解析、校验与求值。

过滤表达式用于在盘点（``list``）或双桶比对（``diff``）时按对象元数据筛掉
不需要关注的对象。为避免引入通用脚本引擎带来的执行风险，这里用一个受控的
递归下降解析器实现一套**极简 DSL**：

* 变量：``SOURCE``（源桶对象）、``TARGET``（目标桶对象，仅比对模式可用）；
* 属性：``size``、``last_modified``（均为整数，后者为 Unix 秒）；
* 字面量：十进制整数；
* 运算符：``==`` ``!=`` ``>`` ``>=`` ``<`` ``<=``（比较）、
  ``&&`` ``||`` ``!``（逻辑）、``(`` ``)``（分组）。

任何超出上述范围的写法都会在**编译期**被拒绝，并在日志中给出具体位置，
从而把「表达式写错」与「对象不匹配」两种情况清晰地区分开。

示例::

    SOURCE.size > 1000 && SOURCE.last_modified >= 1704038400
    (SOURCE.size > 1000 && SOURCE.last_modified >= 1704038400) || TARGET.size == 100000
"""

from __future__ import annotations

import re
from typing import Callable, Iterable, Optional, Sequence

from .model import ObjectProps

#: 允许引用的变量名
ALLOWED_VARIABLES = ("SOURCE", "TARGET")
#: 允许访问的属性名
ALLOWED_PROPERTIES = ("size", "last_modified")

_TOKEN_RE = re.compile(
    r"""
      (?P<space>\s+)
    | (?P<number>\d+)
    | (?P<ident>[A-Za-z_][A-Za-z_0-9]*)
    | (?P<op>&&|\|\||==|!=|>=|<=|>|<|!|\(|\)|\.)
    """,
    re.VERBOSE,
)


class FilterSyntaxError(ValueError):
    """表达式不符合受支持的语法。"""


class _Token:
    __slots__ = ("kind", "text", "pos")

    def __init__(self, kind: str, text: str, pos: int) -> None:
        self.kind = kind
        self.text = text
        self.pos = pos

    def __repr__(self) -> str:  # pragma: no cover - 仅用于调试
        return f"_Token({self.kind!r}, {self.text!r}, {self.pos})"


def _tokenize(expr: str) -> list[_Token]:
    tokens: list[_Token] = []
    pos = 0
    size = len(expr)
    while pos < size:
        m = _TOKEN_RE.match(expr, pos)
        if m is None:
            raise FilterSyntaxError(f"第 {pos} 个字符处出现无法识别的内容: {expr[pos]!r}")
        pos = m.end()
        kind = m.lastgroup
        if kind == "space":
            continue
        tokens.append(_Token(kind, m.group(), m.start()))
    return tokens


# ---------------------------------------------------------------------------
# AST 节点
# ---------------------------------------------------------------------------


class _Node:
    def eval(self, source: ObjectProps, target: Optional[ObjectProps]) -> bool:
        raise NotImplementedError


class _Literal(_Node):
    def __init__(self, value: int) -> None:
        self.value = value

    def eval(self, source: ObjectProps, target: Optional[ObjectProps]) -> bool:
        raise FilterSyntaxError("整数字面量不能单独作为条件")


class _Property(_Node):
    def __init__(self, variable: str, prop: str, pos: int) -> None:
        if variable not in ALLOWED_VARIABLES:
            raise FilterSyntaxError(
                f"第 {pos} 个字符处引用了不允许的变量 {variable!r}，"
                f"仅支持 {', '.join(ALLOWED_VARIABLES)}"
            )
        if prop not in ALLOWED_PROPERTIES:
            raise FilterSyntaxError(
                f"第 {pos} 个字符处引用了不允许的属性 {prop!r}，"
                f"仅支持 {', '.join(ALLOWED_PROPERTIES)}"
            )
        self.variable = variable
        self.prop = prop
        self.pos = pos

    def value_of(self, source: ObjectProps, target: Optional[ObjectProps]) -> int:
        if self.variable == "SOURCE":
            holder = source
        else:
            if target is None:
                raise FilterSyntaxError(
                    "TARGET 仅在双桶比对模式下可用；单桶盘点请改用 SOURCE"
                )
            holder = target
        return int(getattr(holder, self.prop))

    def eval(self, source: ObjectProps, target: Optional[ObjectProps]) -> bool:
        raise FilterSyntaxError("属性引用不能单独作为条件，需要与比较运算符组合")


class _Compare(_Node):
    _OPS: dict[str, Callable[[int, int], bool]] = {
        "==": lambda a, b: a == b,
        "!=": lambda a, b: a != b,
        ">": lambda a, b: a > b,
        ">=": lambda a, b: a >= b,
        "<": lambda a, b: a < b,
        "<=": lambda a, b: a <= b,
    }

    def __init__(self, op: str, left: _Node, right: _Node) -> None:
        self.op = op
        self.left = left
        self.right = right

    def eval(self, source: ObjectProps, target: Optional[ObjectProps]) -> bool:
        left = self._operand(self.left, source, target)
        right = self._operand(self.right, source, target)
        return self._OPS[self.op](left, right)

    @staticmethod
    def _operand(node: _Node, source: ObjectProps, target: Optional[ObjectProps]) -> int:
        if isinstance(node, _Property):
            return node.value_of(source, target)
        if isinstance(node, _Literal):
            return node.value
        raise FilterSyntaxError("比较运算符两侧必须是属性或整数字面量")


class _Not(_Node):
    def __init__(self, inner: _Node) -> None:
        self.inner = inner

    def eval(self, source: ObjectProps, target: Optional[ObjectProps]) -> bool:
        return not self.inner.eval(source, target)


class _Binary(_Node):
    def __init__(self, op: str, left: _Node, right: _Node) -> None:
        self.op = op
        self.left = left
        self.right = right

    def eval(self, source: ObjectProps, target: Optional[ObjectProps]) -> bool:
        if self.op == "&&":
            return self.left.eval(source, target) and self.right.eval(source, target)
        return self.left.eval(source, target) or self.right.eval(source, target)


# ---------------------------------------------------------------------------
# 解析器
# ---------------------------------------------------------------------------


class _Parser:
    def __init__(self, tokens: Sequence[_Token], expr: str) -> None:
        self._tokens = list(tokens)
        self._expr = expr
        self._idx = 0

    # -- 基础操作 -----------------------------------------------------------

    def _peek(self) -> Optional[_Token]:
        return self._tokens[self._idx] if self._idx < len(self._tokens) else None

    def _accept(self, text: str) -> bool:
        tok = self._peek()
        if tok is not None and tok.kind == "op" and tok.text == text:
            self._idx += 1
            return True
        return False

    def _expect(self, text: str) -> None:
        if not self._accept(text):
            tok = self._peek()
            where = f"第 {tok.pos} 个字符处" if tok else "表达式末尾"
            raise FilterSyntaxError(f"{where}期望 {text!r}")

    # -- 文法 ---------------------------------------------------------------

    def parse(self) -> _Node:
        node = self._parse_or()
        if self._peek() is not None:
            tok = self._peek()
            assert tok is not None
            raise FilterSyntaxError(f"第 {tok.pos} 个字符处存在多余内容: {tok.text!r}")
        return node

    def _parse_or(self) -> _Node:
        node = self._parse_and()
        while self._accept("||"):
            node = _Binary("||", node, self._parse_and())
        return node

    def _parse_and(self) -> _Node:
        node = self._parse_unary()
        while self._accept("&&"):
            node = _Binary("&&", node, self._parse_unary())
        return node

    def _parse_unary(self) -> _Node:
        if self._accept("!"):
            return _Not(self._parse_unary())
        return self._parse_primary()

    def _parse_primary(self) -> _Node:
        if self._accept("("):
            node = self._parse_or()
            self._expect(")")
            return node

        left = self._parse_operand()
        tok = self._peek()
        if tok is None or tok.kind != "op" or tok.text not in _Compare._OPS:
            where = f"第 {tok.pos} 个字符处" if tok else "表达式末尾"
            raise FilterSyntaxError(f"{where}缺少比较运算符（== != > >= < <=）")
        self._idx += 1
        right = self._parse_operand()
        return _Compare(tok.text, left, right)

    def _parse_operand(self) -> _Node:
        tok = self._peek()
        if tok is None:
            raise FilterSyntaxError("表达式在期望操作数处结束")
        if tok.kind == "number":
            self._idx += 1
            return _Literal(int(tok.text))
        if tok.kind == "ident":
            self._idx += 1
            variable = tok.text
            self._expect(".")
            prop_tok = self._peek()
            if prop_tok is None or prop_tok.kind != "ident":
                raise FilterSyntaxError(f"第 {tok.pos} 个字符处 {variable!r} 后缺少属性名")
            self._idx += 1
            return _Property(variable, prop_tok.text, tok.pos)
        raise FilterSyntaxError(f"第 {tok.pos} 个字符处出现意外的 token: {tok.text!r}")


# ---------------------------------------------------------------------------
# 对外接口
# ---------------------------------------------------------------------------


class ObjectFilter:
    """已编译的对象过滤表达式。

    比对模式下的判定入口是 :meth:`match_source_target`，单桶模式下的入口是
    :meth:`matches`——与两条数据通路的实际语义保持一致：

    * 单桶盘点：仅当对象自身满足条件才导出；
    * 双桶比对：仅当**两侧对象同时满足**条件才参与差异导出（两侧任一不满足即
      整体忽略），对应 ``DiffFlag=3`` 的场景。
    """

    def __init__(self, expr: str, mode: str = "list") -> None:
        self.expr = expr
        tokens = _tokenize(expr)
        if not tokens:
            raise FilterSyntaxError("过滤表达式为空")
        self._ast = _Parser(tokens, expr).parse()
        self._uses_target = any(
            tok.kind == "ident" and tok.text == "TARGET" for tok in tokens
        )
        if self._uses_target and mode != "diff":
            raise FilterSyntaxError("TARGET 仅在双桶比对（diff）模式下可用")

        # 编译期自检：用全零样例跑一遍，暴露「类型不通」「除零」等运行期才会
        # 出现的问题，避免在百万对象的导出阶段才报错。
        probe = ObjectProps()
        try:
            self._ast.eval(probe, probe if self._uses_target else None)
        except FilterSyntaxError:
            raise
        except Exception as exc:  # pragma: no cover - 兜底
            raise FilterSyntaxError(f"过滤表达式自检失败: {exc}") from exc

    # -- 单桶模式 -----------------------------------------------------------

    def matches(self, obj: ObjectProps) -> bool:
        """单桶盘点：对象自身是否满足条件。"""
        return bool(self._ast.eval(obj, None))

    # -- 比对模式 -----------------------------------------------------------

    def match_source_target(self, source: ObjectProps, target: ObjectProps) -> bool:
        """双桶比对：两侧对象是否**同时**满足条件。"""
        return bool(self._ast.eval(source, target))

    def __call__(self, *objects: ObjectProps) -> bool:
        """自适应调用：1 个参数按单桶语义，2 个参数按比对语义。"""
        if len(objects) == 1:
            return self.matches(objects[0])
        if len(objects) == 2:
            return self.match_source_target(objects[0], objects[1])
        raise TypeError("过滤判定需要 1 个（单桶）或 2 个（比对）对象")

    def referenced_properties(self) -> Iterable[str]:
        """返回表达式中实际用到的属性名，便于在日志中提示。"""
        return tuple(self._collect_props(self._ast))

    @staticmethod
    def _collect_props(node: _Node) -> list[str]:
        if isinstance(node, _Property):
            return [node.prop]
        if isinstance(node, _Compare):
            return ObjectFilter._collect_props(node.left) + ObjectFilter._collect_props(node.right)
        if isinstance(node, _Not):
            return ObjectFilter._collect_props(node.inner)
        if isinstance(node, _Binary):
            return ObjectFilter._collect_props(node.left) + ObjectFilter._collect_props(node.right)
        return []


def compile_filter(expr: Optional[str], mode: str = "list") -> Optional[ObjectFilter]:
    """编译过滤表达式；``expr`` 为空时返回 ``None``。"""
    if expr is None or not expr.strip():
        return None
    return ObjectFilter(expr, mode)
