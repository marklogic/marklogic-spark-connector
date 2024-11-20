function transform(context, params, content) {
  fn.error(xs.QName("ERROR"), "This is an intentional error for testing purposes.");
  return content;
};

exports.transform = transform;
