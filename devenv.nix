{ pkgs, lib, ... }:
{
  # PySpark runs its Spark engine on the JVM, even when the Python
  # application is executed locally.
  packages = [ pkgs.jdk17 ];
  env.JAVA_HOME = "${pkgs.jdk17}";

  # Put the uv-managed virtualenv at the standard .venv location so that
  # editors (Zed, VS Code, ...) can auto-detect it. devenv's default puts it
  # in .devenv/state/venv which IDEs do not scan. mkForce is required here:
  # the python module also sets UV_PROJECT_ENVIRONMENT.
  env.UV_PROJECT_ENVIRONMENT = lib.mkForce ".venv";

  languages.python = {
    enable = true;
    uv.enable = true;
  };
}