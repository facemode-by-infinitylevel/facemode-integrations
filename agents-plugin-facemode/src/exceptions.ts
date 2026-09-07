export class FaceModeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'FaceModeError';
  }
}

export class FaceModeApiError extends FaceModeError {
  constructor(message: string) {
    super(message);
    this.name = 'FaceModeApiError';
  }
}

export class FaceModeProtocolError extends FaceModeError {
  constructor(message: string) {
    super(message);
    this.name = 'FaceModeProtocolError';
  }
}
