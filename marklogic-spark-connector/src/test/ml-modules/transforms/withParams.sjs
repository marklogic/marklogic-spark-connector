function transform(context, params, content)
{
  return {
    "content": content,
    "params": params,
    "context": context
  }
};
exports.transform = transform;
