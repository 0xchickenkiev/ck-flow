USE [master]
GO

/****** Object:  Database [WH_Control]    Script Date: 06/02/2023 14:59:10 ******/
CREATE DATABASE [WH_Control]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'Control', FILENAME = N'F:\Data\BSG\Control.mdf' , SIZE = 1048576KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1048576KB ), 
 FILEGROUP [Control_Default]  DEFAULT
( NAME = N'Control_Default', FILENAME = N'F:\Data\BSG\Control_Default.ndf' , SIZE = 17825792KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1048576KB )
 LOG ON 
( NAME = N'Control_Log', FILENAME = N'G:\Logs\BSG\Control.ldf' , SIZE = 1048576KB , MAXSIZE = 2048GB , FILEGROWTH = 1048576KB )
GO

IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [WH_Control].[dbo].[sp_fulltext_database] @action = 'enable'
end
GO

ALTER DATABASE [WH_Control] SET ANSI_NULL_DEFAULT ON 
GO

ALTER DATABASE [WH_Control] SET ANSI_NULLS ON 
GO

ALTER DATABASE [WH_Control] SET ANSI_PADDING ON 
GO

ALTER DATABASE [WH_Control] SET ANSI_WARNINGS ON 
GO

ALTER DATABASE [WH_Control] SET ARITHABORT ON 
GO

ALTER DATABASE [WH_Control] SET AUTO_CLOSE OFF 
GO

ALTER DATABASE [WH_Control] SET AUTO_SHRINK OFF 
GO

ALTER DATABASE [WH_Control] SET AUTO_UPDATE_STATISTICS ON 
GO

ALTER DATABASE [WH_Control] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO

ALTER DATABASE [WH_Control] SET CURSOR_DEFAULT  LOCAL 
GO

ALTER DATABASE [WH_Control] SET CONCAT_NULL_YIELDS_NULL ON 
GO

ALTER DATABASE [WH_Control] SET NUMERIC_ROUNDABORT OFF 
GO

ALTER DATABASE [WH_Control] SET QUOTED_IDENTIFIER ON 
GO

ALTER DATABASE [WH_Control] SET RECURSIVE_TRIGGERS OFF 
GO

ALTER DATABASE [WH_Control] SET  DISABLE_BROKER 
GO

ALTER DATABASE [WH_Control] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO

ALTER DATABASE [WH_Control] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO

ALTER DATABASE [WH_Control] SET TRUSTWORTHY OFF 
GO

ALTER DATABASE [WH_Control] SET ALLOW_SNAPSHOT_ISOLATION OFF 
GO

ALTER DATABASE [WH_Control] SET PARAMETERIZATION SIMPLE 
GO

ALTER DATABASE [WH_Control] SET READ_COMMITTED_SNAPSHOT OFF 
GO

ALTER DATABASE [WH_Control] SET HONOR_BROKER_PRIORITY OFF 
GO

ALTER DATABASE [WH_Control] SET RECOVERY FULL 
GO

ALTER DATABASE [WH_Control] SET  MULTI_USER 
GO

ALTER DATABASE [WH_Control] SET PAGE_VERIFY CHECKSUM  
GO

ALTER DATABASE [WH_Control] SET DB_CHAINING OFF 
GO

ALTER DATABASE [WH_Control] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
GO

ALTER DATABASE [WH_Control] SET TARGET_RECOVERY_TIME = 0 SECONDS 
GO

ALTER DATABASE [WH_Control] SET DELAYED_DURABILITY = DISABLED 
GO

ALTER DATABASE [WH_Control] SET QUERY_STORE = OFF
GO

USE [WH_Control]
GO

ALTER DATABASE SCOPED CONFIGURATION SET LEGACY_CARDINALITY_ESTIMATION = OFF;
GO

ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 0;
GO

ALTER DATABASE SCOPED CONFIGURATION SET PARAMETER_SNIFFING = ON;
GO

ALTER DATABASE SCOPED CONFIGURATION SET QUERY_OPTIMIZER_HOTFIXES = OFF;
GO

ALTER DATABASE [WH_Control] SET  READ_WRITE 
GO

