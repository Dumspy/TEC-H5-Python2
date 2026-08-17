{ pkgs, ... }:
{
  # PySpark runs its Spark engine on the JVM, even when the Python
  # application is executed locally.
  packages = [ pkgs.jdk17 ];
  env.JAVA_HOME = "${pkgs.jdk17}";

  languages.python = {
    enable = true;
    uv.enable = true;
  };
}
